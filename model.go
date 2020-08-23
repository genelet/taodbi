package taodbi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
)

// Navigate is interface to implement Model
//
type Navigate interface {
	// GetAction: get an action function by name
	GetAction(string) func(...map[string]interface{}) error

	// GetLists: get the main data
	GetLists() []map[string]interface{}

	// getArgs: get ARGS; pass "true" for nextpages
	getArgs(...bool) map[string]interface{}

	// SetArgs: set new input
	SetArgs(map[string]interface{})

	// getNextpages: get the nextpages
	getNextpages(string) []*Page

	// SetDB: set SQL handle
	SetDB(*sql.DB)
}

// Model works on table's CRUD in web applications.
//
type Model struct {
	Crud
	Navigate
	Updated bool

	InsupdPars []string `json:"insupd_pars"`
	// Actions: map between name and action functions
	Actions map[string]func(...map[string]interface{}) error  `json:"-"`
	// aARGS: the input data received by the web request
	aARGS map[string]interface{}
	// aLISTS: output data as slice of map, which represents a table row
	aLISTS []map[string]interface{}
}

// NewModel creates a new Model struct from json file 'filename'
// You should use SetDB to assign a database handle and
// SetArgs to set input data, a url.Value, to make it working
//
func NewModel(filename string) (*Model, error) {
    content, err := ioutil.ReadFile(filename)
    if err != nil { return nil, err }
    var parsed *Model
    if err := json.Unmarshal(content, &parsed); err != nil {
        return nil, err
    }
    parsed.Crud.fulfill()
	parsed.acrud = parsed

    return parsed, nil
}

// GetLists get main data as slice of mapped row
func (self *Model) GetLists() []map[string]interface{} {
	return self.aLISTS
}

// GetAction returns action's function
func (self *Model) GetAction(action string) func(...map[string]interface{}) error {
	if act, ok := self.Actions[action]; ok {
		return act
	}

	return nil
}

// getArgs returns the input data which may have extra keys added
// pass true will turn off those pagination data
func (self *Model) getArgs(is ...bool) map[string]interface{} {
	args := map[string]interface{}{}
	for k, v := range self.aARGS {
		if is != nil && is[0] && grep([]string{self.Sortby, self.Totalno, self.Pageno, self.Sortreverse, self.Rowcount, self.Passid}, k) {
			continue
		}
		args[k] = v
	}

	return args
}

// SetArgs sets input data
func (self *Model) SetArgs(args map[string]interface{}) {
	self.aARGS = args
}

// getNextpages returns the next pages of an action
func (self *Model) getNextpages(action string) []*Page {
	if !hasValue(self.Nextpages) {
		return nil
	}
	nps, ok := self.Nextpages[action]
	if !ok {
		return nil
	}
	return nps
}

// SetDB sets the DB handle
func (self *Model) SetDB(db *sql.DB) {
	self.Crud.DB = db
	self.aLISTS = make([]map[string]interface{}, 0)
}

func (self *Model) filteredFields(pars []string) []string {
	ARGS := self.aARGS
	fields, ok := ARGS[self.Fields]
	if !ok {
		return pars
	}

	out := make([]string, 0)
	for _, field := range fields.([]string) {
		for _, v := range pars {
			if field == v {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

func (self *Model) getFv(pars []string) map[string]interface{} {
	ARGS := self.aARGS
	fieldValues := make(map[string]interface{})
	for _, f := range self.filteredFields(pars) {
		if v, ok := ARGS[f]; ok {
			fieldValues[f] = v
		}
	}
	return fieldValues
}

func (self *Model) editIdVal(extra ...map[string]interface{}) []interface{} {
	if hasValue(extra) {
		return []interface{}{self.properValue(self.CurrentKey, extra[0])}
	}
	return []interface{}{self.properValue(self.CurrentKey, nil)}
}

func (self *Model) editFKVal(extra ...map[string]interface{}) []interface{} {
	if hasValue(extra) {
		return []interface{}{self.properValue(self.ForeignKey, extra[0])}
	}
	return []interface{}{self.properValue(self.ForeignKey, nil)}
}

// properValue returns the value of key 'v' from extra.
// In case it does not exist, it tries to get from ARGS.
func (self *Model) properValue(v string, extra map[string]interface{}) interface{} {
	ARGS := self.aARGS
	if !hasValue(extra) {
		return ARGS[v]
	}
	if val, ok := extra[v]; ok {
		return val
	}
	return ARGS[v]
}

// properValues returns the values of multiple keys 'vs' from extra.
// In case it does not exists, it tries to get from ARGS.
func (self *Model) properValues(vs []string, extra map[string]interface{}) []interface{} {
	ARGS := self.aARGS
	outs := make([]interface{}, len(vs))
	if !hasValue(extra) {
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

// properValuesHash is the same as properValues, but resulting in a map.
func (self *Model) properValuesHash(vs []string, extra map[string]interface{}) map[string]interface{} {
	values := self.properValues(vs, extra)
	hash := make(map[string]interface{})
	for i, v := range vs {
		hash[v] = values[i]
	}
	return hash
}

// Topics selects many rows, optionally with restriction defined in 'extra'.
func (self *Model) Topics(extra ...map[string]interface{}) error {
	ARGS := self.aARGS
	totalForce := self.TotalForce // 0 means no total calculation
	_, ok1 := ARGS[self.Rowcount]
	pageno, _ := ARGS[self.Pageno]
	totalno, ok3 := ARGS[self.Totalno]
	if totalForce != 0 && ok1 && (!ok3 || pageno.(int) == 1) {
        nt := 0
        if totalForce < -1 { // take the absolute as the total number
            nt = int(math.Abs(float64(totalForce)))
        } else if totalForce == -1 || !ok3 { // optionally cal
            if err := self.totalHash(&nt, extra...); err != nil {
                return err
            }
        } else {
            nt = totalno.(int)
        }
        ARGS[self.Totalno] = nt
    }

	hashPars := self.topicsHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(self.TopicsHash, self.TopicsPars, fields.([]string))
    }

	self.aLISTS = make([]map[string]interface{}, 0)
	return self.topicsHash(&self.aLISTS, hashPars, self.orderString(), extra...)
}

// orderString outputs the ORDER BY string using information in args
func (self *Model) orderString() string {
    ARGS := self.aARGS
    column := self.CurrentKey
    if sortby, ok := ARGS[self.Sortby]; ok {
        column = sortby.(string)
    }

    order := "ORDER BY " + column
    if _, ok := ARGS[self.Sortreverse]; ok {
        order += " DESC"
    }
    if Rowcount, ok := ARGS[self.Rowcount]; ok {
		rowcount := Rowcount.(int)
        pageno := 1
        if Pageno, ok := ARGS[self.Pageno]; ok {
			pageno = Pageno.(int)
        }
        order += " LIMIT " + fmt.Sprintf("%d", rowcount) + " OFFSET " + fmt.Sprintf("%d", (pageno-1)*rowcount)
    }

    matched, err := regexp.MatchString("[;'\"]", order)
    if err != nil || matched {
        return ""
    }
    return order
}

// Edit selects few rows (usually one) using primary key value in ARGS,
// optionally with restrictions defined in 'extra'.
func (self *Model) Edit(extra ...map[string]interface{}) error {
	val := self.editIdVal(extra...)
	if !hasValue(val) {
		return errors.New("pk value not provided")
	}

	hashPars := self.editHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(self.EditHash, self.EditPars, fields.([]string))
    }

	self.aLISTS = make([]map[string]interface{}, 0)
	return self.editHash(&self.aLISTS, hashPars, val, extra...)
}

// EditFK selects ony one using 'foreign key' value in ARGS,
// optionally with restrictions defined in 'extra'.
func (self *Model) EditFK(extra ...map[string]interface{}) error {
    id := self.ForeignKey
    val := self.aARGS[id]
    if hasValue(extra) {
        val = self.properValue(id, extra[0])
    }
    if val == nil {
        return errors.New("Foreign key has no value")
    }

	hashPars := self.editHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(self.EditHash, self.EditPars, fields.([]string))
    }

    self.aLISTS = make([]map[string]interface{}, 0)
    return self.editHashFK(&self.aLISTS, hashPars, []interface{}{val}, extra...)
}

// Insert inserts a row using data passed in ARGS. Any value defined
// in 'extra' will override that in ARGS and be used for that column.
func (self *Model) Insert(extra ...map[string]interface{}) error {
	fieldValues := self.getFv(self.InsertPars)
	if hasValue(extra) {
		for key, value := range extra[0] {
			if grep(self.InsertPars, key) {
				fieldValues[key] = value
			}
		}
	}
	if !hasValue(fieldValues) {
		return errors.New("no data to insert")
	}

	if err := self.insertHash(fieldValues); err != nil {
		return err
	}

	fieldValues[self.CurrentKey] = self.LastID
	self.aARGS[self.CurrentKey] = self.LastID
	self.aLISTS = make([]map[string]interface{}, 0)
	self.aLISTS = append(self.aLISTS, fieldValues)

	return nil
}

// Insupd inserts a new row if it does not exist, or retrieves the old one,
// depending on the unique of the columns defined in InsupdPars.
func (self *Model) Insupd(extra ...map[string]interface{}) error {
	fieldValues := self.getFv(self.InsupdPars)
	if hasValue(extra) {
		for key, value := range extra[0] {
			if grep(self.InsertPars, key) {
				fieldValues[key] = value
			}
		}
	}
	if !hasValue(fieldValues) {
		return errors.New("unique value not found")
	}

	lists := make([]map[string]interface{}, 0)
    if err := self.topicsHash(&lists, self.CurrentKey, "", fieldValues); err != nil {
        return err
    }

	if len(lists) > 1 {
        return errors.New("multiple returns for unique key")
    }

	args := self.properValuesHash(self.InsertPars, nil)
	if len(lists) == 1 {
		self.Updated = true
        args[self.CurrentKey] = lists[0][self.CurrentKey]
	}
	if err := self.insertHash(args); err != nil {
		return err
	}

	if self.Updated {
		self.LastID = args[self.CurrentKey].(int64)
	} else {
		args[self.CurrentKey] = self.LastID
	}
	self.aLISTS = append(self.aLISTS, args)

	return nil
}
