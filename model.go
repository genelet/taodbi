package taodbi

import (
	//"log"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
)

// Model runs data actions automcally in a database table.
//
// Input data: ARGS, like a http request object.
// Output data: LISTS
// columns to be inserted: InsertPars
// columns to be selected: TopicsPars
// columns to identify if row exists: InsupdPars
//
// The following parameters are used for pagination.
// Their values are defined in ARGS.
// ARGS[ROWCOUNT]: rows per page. The default ROWCOUNT is 'rowcount' in ARGS.
// ARGS[PAGENO]: which page number to request. The default name 'pageno'.
// ARGS[TOTALNO]: total number of rows available. The default name 'totalno'.
// ARGS[SORTBY]: to sort data by this column. The default name 'sort'.
// ARGS[SORTREVERSE]: if the reversed sorting. The default name 'sortreverse'.
//
// Search multiple tables: in many applications one need data
// involving multiple tables. TDengine does not support 'JOIN' so
// this feature is handy to use for JOIN-like operations.
// Assume the original search is a shopping list in this season in model 'shopping'.
// model "testing2" represents detail of each shopping, and 
// model "testing3" represents the definition of products.
// "nextpages": {
//    "topics" : [
//      {"model":"testing2", "action": "topics", "relate_item":{"k":"v"}},
//      {"model":"testing3", "action": "topics"}
//    ] ,
//    "edit" : [...]
// }
// In this example, if one calls function 'Topics', which is to search multiple
// rows, you will trigger another action "Topics" on model "testing2" for each
// row, with the restrition according to 'relate_item'.
// I.e. testing2's column 'v' takes the same as column 'k' in the original rows.
// Results from 'testing2' are saved under the key name 'tesing2_topics'.
//
// It will also run once for action 'topics' on model 'testing3'. Because these
// data don't belong to any specific row, they are put in OTHER under 'testing3_topics'.
//
type Model struct {
	Crud `json:"crud,omitempty"`

	ARGS  map[string]interface{}   `json:"args,omitempty"`
	LISTS []map[string]interface{} `json:"lists,omitempty"`
	OTHER map[string]interface{}   `json:"other,omitempty"`

	SORTBY      string `json:"sortby,omitempty"`
	SORTREVERSE string `json:"sortreverse,omitempty"`
	PAGENO      string `json:"pageno,omitempty"`
	ROWCOUNT    string `json:"rowcount,omitempty"`
	TOTALNO     string `json:"totalno,omitempty"`

	Nextpages map[string][]map[string]interface{} `json:"nextpages,omitempty"`
	Storage   map[string]map[string]interface{}   `json:"storage,omitempty"`

	InsertPars []string `json:"insert_pars,omitempty"`
	InsupdPars []string `json:"insupd_Pars,omitempty"`

	EditPars   []string          `json:"edit_pars,omitempty"`
	TopicsPars []string          `json:"topics_pars,omitempty"`
	EditMap    map[string]string `json:"edit_map,omitempty"`
	TopicsMap  map[string]string `json:"topics_map,omitempty"`

	TotalForce int `json:"total_force,omitempty"`
}

// NewModel constructs a new Model object from the json model string
func NewModel(content []byte) (*Model, error) {
	parsed := new(Model)
	err := json.Unmarshal(content, parsed)
	if err != nil {
		return nil, err
	}

	if parsed.SORTBY == "" {
		parsed.SORTBY = "sortby"
	}
	if parsed.SORTREVERSE == "" {
		parsed.SORTREVERSE = "sortreverse"
	}
	if parsed.PAGENO == "" {
		parsed.PAGENO = "pageno"
	}
	if parsed.ROWCOUNT == "" {
		parsed.ROWCOUNT = "rowcount"
	}
	if parsed.TOTALNO == "" {
		parsed.TOTALNO = "totalno"
	}

	return parsed, nil
}

func (self *Model) filteredFields(in_hash map[string]string, in_pars []string) interface{} {
	if in_hash != nil {
		return in_hash
	}
	return in_pars
}

// Topics selects many rows, optionally with restriction defined in 'extra'.
func (self *Model) Topics(extra ...map[string]interface{}) error {
	fields := self.filteredFields(self.TopicsMap, self.TopicsPars)
	order, err := self.OrderString(extra...)
	if err != nil {
		return err
	}
	self.LISTS = make([]map[string]interface{}, 0)
	err = self.TopicsHashOrder(&self.LISTS, fields, order, extra...)
	if err != nil {
		return err
	}

	return self.ProcessAfter("topics", extra...)
}

// Edit selects few rows (usually one) using primary key value in ARGS,
// optionally with restrictions defined in 'extra'.
func (self *Model) Edit(extra ...map[string]interface{}) error {
	id := self.CurrentKey
	val := self.ARGS[id]
	if hasValue(extra...) {
		val = self.ProperValue(id, extra[0])
	}
	if val == nil {
		return errors.New("Primay key has no value")
	}

	fields := self.filteredFields(self.EditMap, self.EditPars)

	self.LISTS = make([]map[string]interface{}, 0)
	err := self.EditHash(&self.LISTS, fields, val, extra...)
	if err != nil {
		return err
	}

	return self.ProcessAfter("edit", extra...)
}

func index(vs []string, t string) int {
	for i, v := range vs {
		if v == t {
			return i
		}
	}
	return -1
}

// Insert inserts a row using data passed in ARGS. Any value defined
// in 'extra' will override that in ARGS and be used for that column.
func (self *Model) Insert(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil {
		one = extra[0]
	}
	field_values := self.ProperValuesHash(self.InsertPars, one)

	err := self.InsertLast(field_values)
	if err != nil {
		return err
	}

	self.LISTS = []map[string]interface{}{self.CurrentRow}

	return self.ProcessAfter("insert", extra...)
}

// Insupd inserts a new row if it does not exist, or retrieves the old one,
// depending on the unique of the columns defined in InsupdPars.
func (self *Model) Insupd(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil {
		one = extra[0]
	}
	field_values := self.ProperValuesHash(self.InsertPars, one)

	uniques := self.InsupdPars
	if uniques == nil {
		return errors.New("Unique columns not defined")
	}
	for _, v := range uniques {
		if _, ok := field_values[v]; !ok {
			return errors.New("Missing unique column value")
		}
	}

	err := self.InsupdHash(field_values, uniques)
	if err != nil {
		return err
	}
	self.LISTS = []map[string]interface{}{self.CurrentRow}

	return self.ProcessAfter("insupd", extra...)
}

/*
func (self *Model) existing(table string, field string, val interface{}) error {
 	one := 0
 	return self.Db.QueryRow("SELECT 1 FROM "+table+" WHERE "+field+"=?").Scan(&one)
}

func (self *Model) randomID(table string, field string, m ...interface{}) error {
    min := 0
    max := 4294967295
    trials := 10
    if m != nil {
        min = m[0].(int)
        max = m[1].(int)
        if m[2] != nil {
            trials = m[2].(int)
        }
    }

    for i := 0; i < trials; i++ {
        val := min + int(rand.Float32()*float32(max-min))
        err := self.existing(table, field, val)
        if err != nil {
            continue
        }
        self.ARGS[field] = val
        return nil
    }
    return errors.New("1076")
}
*/


// OrderString returns the SQL statement for sorting and pagination.
func (self *Model) OrderString(extra ...map[string]interface{}) (string, error) {
	ARGS := self.ARGS
	TOTALNO := self.TOTALNO
	PAGENO := self.PAGENO
	ROWCOUNT := self.ROWCOUNT
	SORTBY := self.SORTBY
	SORTREVERSE := self.SORTREVERSE

	column := self.CurrentKey
	if v, ok := ARGS[SORTBY]; ok {
		column = v.(string)
		if strings.Contains(column, `;'"`) {
			return "", errors.New("Incorrect characters in SQL")
		}
	}
	if _, ok := ARGS[SORTREVERSE]; ok {
		column += " DESC"
	}

	nr, ok1 := ARGS[ROWCOUNT]
	page_no, ok2 := ARGS[PAGENO]
	if ok1 {
		rowcount := nr.(int)
		pageno := 1
		if ok2 {
			pageno = page_no.(int)
		}
		column += " LIMIT " + strconv.Itoa(rowcount) + " OFFSET " + strconv.Itoa((pageno-1)*rowcount)
		nt := 0
		if self.TotalForce < -1 {
			nt = int(math.Abs(float64(self.TotalForce)))
		} else if self.TotalForce == -1 || ARGS[TOTALNO] == nil {
			if err := self.TotalHash(&nt, extra...); err != nil {
				return "", err
			}
		}
		self.ARGS[TOTALNO] = nt
		self.OTHER[TOTALNO] = nt
	}

	return column, nil
}

func (self *Model) anotherObject(item map[string]interface{}, page map[string]interface{}, extra ...map[string]interface{}) error {
	model := page["model"].(string)
	action := page["action"].(string)

	m, ok := self.Storage["model"][model]
	if !ok {
		return errors.New("Model not defined in nextpage")
	}
	model_obj := m.(*Model)
	action_funcs, ok := self.Storage["action"][model]
	if !ok {
		return errors.New("Action not defined in nextpage")
	}

	marker := model + "_" + action
	if alias, ok := page["alias"]; ok {
		marker = alias.(string)
	}
	if page["ignore"] != nil && item[marker] != nil {
		return nil
	}

	args := make(map[string]interface{})
	for k, v := range self.ARGS {
		if index([]string{self.SORTBY, self.SORTREVERSE, self.ROWCOUNT, self.TOTALNO, self.PAGENO}, k) >= 0 {
			continue
		}
		args[k] = v
	}
	model_obj.ARGS = args // this delivers ARGS to the real action-object below

	var hash map[string]interface{}
	if hasValue(extra...) {
		for k, v := range extra[0] {
			if hash == nil {
				hash = make(map[string]interface{})
			}
			hash[k] = v
		}
	}
	if u, ok := page["manual"]; ok {
		for k, v := range u.(map[string]interface{}) {
			if hash == nil {
				hash = make(map[string]interface{})
			}
			hash[k] = v
		}
	}
	if hash != nil {
		if hasValue(extra...) {
			extra[0] = hash
		} else {
			extra = []map[string]interface{}{hash}
		}
	}

	middle_actions := action_funcs.(map[string]interface{})
	if middle_actions == nil {
		return errors.New("Model not defined in nextpage's action map")
	}
	final_action := middle_actions[action].(func(...map[string]interface{}) error)
	if final_action == nil {
		return errors.New("Action not defined in nextpage's action map")
	}
	err := final_action(extra...)
	if err != nil {
		return err
	}

	lists := model_obj.LISTS
	other := model_obj.OTHER

	if len(lists) > 0 {
		item[marker] = lists
	}
	if len(other) > 0 {
		for k, v := range other {
			self.OTHER[k] = v
		}
	}
	model_obj.LISTS = nil
	model_obj.OTHER = nil

	return nil
}

// CallOnce calls another object's action, for once.
func (self *Model) CallOnce(page map[string]interface{}, extra ...map[string]interface{}) error {
	return self.anotherObject(self.OTHER, page, extra...)
}

// CallNextpage calls another object's action, for many times.
func (self *Model) CallNextpage(page map[string]interface{}, extra ...map[string]interface{}) error {
	lists := self.LISTS
	if len(lists) < 1 || page["relate_item"] == nil {
		return nil
	}

	for _, item := range lists {
		this := make(map[string]interface{})
		if hasValue(extra...) {
			for k, v := range extra[0] {
				this[k] = v
			}
		}
		found := false
		for k, v := range page["relate_item"].(map[string]interface{}) {
			if _, ok := item[k]; ok {
				found = true
				this[v.(string)] = item[k]
			}
		}
		if found == false {
			continue
		}
		if hasValue(extra...) {
			extra[0] = this
		} else {
			extra = []map[string]interface{}{this}
		}
		err := self.anotherObject(item, page, extra...)
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessAfter triggers calling other methods automatically
// by using 'nextpages' records in the json string.
func (self *Model) ProcessAfter(action string, extra ...map[string]interface{}) error {
	if self.Nextpages == nil || self.Nextpages[action] == nil {
		return nil
	}

	for _, page := range self.Nextpages[action] {
		if hasValue(extra...) {
			extra = extra[1:]
		}
		var err error
		if page["relate_item"] == nil {
			if hasValue(extra...) {
				err = self.CallOnce(page, extra...)
			} else {
				err = self.CallOnce(page)
			}
		} else {
			if hasValue(extra...) {
				err = self.CallNextpage(page, extra...)
			} else {
				err = self.CallNextpage(page)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ProperValue returns the value of key 'v' from extra.
// In case it does not exist, it tries to get from ARGS.
func (self *Model) ProperValue(v string, extra map[string]interface{}) interface{} {
	ARGS := self.ARGS
	if extra == nil {
		return ARGS[v]
	}
	if val, ok := extra[v]; ok {
		return val
	}
	return ARGS[v]
}

// ProperValues returns the values of multiple keys 'vs' from extra.
// In case it does not exists, it tries to get from ARGS.
func (self *Model) ProperValues(vs []string, extra map[string]interface{}) []interface{} {
	ARGS := self.ARGS
	outs := make([]interface{}, len(vs))
	if extra == nil {
		for i, v := range vs {
			outs[i] = ARGS[v]
		}
		return outs
	}
	for i, v := range vs {
		val, ok := extra[v]
		if ok {
			outs[i] = val
		} else {
			outs[i] = ARGS[v]
		}
	}
	return outs
}

// ProperValuesHash is the same as ProperValues, but resulting in a map.
func (self *Model) ProperValuesHash(vs []string, extra map[string]interface{}) map[string]interface{} {
	values := self.ProperValues(vs, extra)
	hash := make(map[string]interface{})
	for i, v := range vs {
		hash[v] = values[i]
	}
	return hash
}
