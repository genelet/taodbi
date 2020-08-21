package taodbi

import (
	"database/sql"
	"errors"
	"time"
)

// Navigate is interface to implement Rmodel
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

// Rmodel works on table's CRUD in web applications.
//
type Rmodel struct {
	Restful
	Navigate

	// Actions: map between name and action functions
	Actions map[string]func(...map[string]interface{}) error  `json:"-"`
	// aARGS: the input data received by the web request
	aARGS map[string]interface{}
	// aLISTS: output data as slice of map, which represents a table row
	aLISTS []map[string]interface{}
}

// NewRmodel creates a new Rmodel struct from json file 'filename'
// You should use SetDB to assign a database handle and
// SetArgs to set input data, a url.Value, to make it working
//
func NewRmodel(filename string) (*Rmodel, error) {
	parsed, err := newRest(filename)
    if err != nil { return nil, err }
	return &Rmodel{Restful:*parsed}, nil
}

// GetLists get main data as slice of mapped row
func (self *Rmodel) GetLists() []map[string]interface{} {
	return self.aLISTS
}

// GetAction returns action's function
func (self *Rmodel) GetAction(action string) func(...map[string]interface{}) error {
	if act, ok := self.Actions[action]; ok {
		return act
	}

	return nil
}

// getArgs returns the input data which may have extra keys added
// pass true will turn off those pagination data
func (self *Rmodel) getArgs(is ...bool) map[string]interface{} {
	args := map[string]interface{}{}
	for k, v := range self.aARGS {
		if is != nil && is[0] && grep([]string{self.Sortreverse, self.Rowcount, self.Passid}, k) {
			continue
		}
		args[k] = v
	}

	return args
}

// SetArgs sets input data
func (self *Rmodel) SetArgs(args map[string]interface{}) {
	self.aARGS = args
}

// getNextpages returns the next pages of an action
func (self *Rmodel) getNextpages(action string) []*Page {
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
func (self *Rmodel) SetDB(db *sql.DB) {
	self.Crud.DB = db
    self.ProfileTable.DB = db
    self.StatusTable.DB = db
	self.aLISTS = make([]map[string]interface{}, 0)
}

func (self *Rmodel) filteredFields(pars []string) []string {
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

func (self *Rmodel) getFv(pars []string) map[string]interface{} {
	ARGS := self.aARGS
	fieldValues := make(map[string]interface{})
	for _, f := range self.filteredFields(pars) {
		if v, ok := ARGS[f]; ok {
			fieldValues[f] = v
		}
	}
	return fieldValues
}

func (self *Rmodel) getFvWithoutFK(pars []string) map[string]interface{} {
	ARGS := self.aARGS
	fieldValues := make(map[string]interface{})
	for _, f := range self.filteredFields(pars) {
		if v, ok := ARGS[f]; ok {
			fieldValues[f] = v
		}
	}
	return fieldValues
}

func (self *Rmodel) editIdVal(extra ...map[string]interface{}) []interface{} {
	if hasValue(extra) {
		return []interface{}{self.properValue(self.CurrentKey, extra[0])}
	}
	return []interface{}{self.properValue(self.CurrentKey, nil)}
}

// Topics selects many rows, optionally with restriction defined in 'extra'.
func (self *Rmodel) Topics(extra ...map[string]interface{}) error {
	ARGS := self.aARGS
	// totalForce := self.TotalForce // 0 means no total calculation
	rowcount := 100
	if v, ok := ARGS[self.Rowcount]; ok {
		rowcount = v.(int)
	}
	reverse := false
	if _, ok := ARGS[self.Sortreverse]; ok {
		reverse = true
	}
	passid := int64(0)
	if v, ok := ARGS[self.Passid]; ok {
		passid = v.(int64)
	} else if reverse {
		passid = time.Now().UnixNano() / int64(time.Microsecond)
	}

	p := self.ProfileTable
	hashPars := p.topicsHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(p.TopicsHash, p.TopicsPars, fields.([]string))
    }

	self.aLISTS = make([]map[string]interface{}, 0)
	return self.topicsRest(rowcount, reverse, passid, &self.aLISTS, hashPars, extra...)
}

// Edit selects few rows (usually one) using primary key value in ARGS,
// optionally with restrictions defined in 'extra'.
func (self *Rmodel) Edit(extra ...map[string]interface{}) error {
	val := self.editIdVal(extra...)
	if !hasValue(val) {
		return errors.New("pk value not provided")
	}

	p := self.ProfileTable
	hashPars := p.editHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(p.EditHash, p.EditPars, fields.([]string))
    }

	self.aLISTS = make([]map[string]interface{}, 0)
	return self.editRest(&self.aLISTS, hashPars, val, extra...)
}

// Insert inserts a row using data passed in ARGS. Any value defined
// in 'extra' will override that in ARGS and be used for that column.
func (self *Rmodel) Insert(extra ...map[string]interface{}) error {
	p := self.ProfileTable
	fieldValues := self.getFv(p.InsertPars)
	if hasValue(extra) {
		for key, value := range extra[0] {
			if grep(p.InsertPars, key) {
				fieldValues[key] = value
			}
		}
	}
	if !hasValue(fieldValues) {
		return errors.New("no data to insert")
	}

	self.aLISTS = make([]map[string]interface{}, 0)
	if err := self.insertRest(fieldValues); err != nil {
		return err
	}

	fieldValues[self.CurrentKey] = self.LastID
	self.aARGS[self.CurrentKey] = self.LastID
	self.aLISTS = append(self.aLISTS, fieldValues)

	return nil
}

// Insupd inserts a new row if it does not exist, or retrieves the old one,
// depending on the unique of the columns defined in InsupdPars.
func (self *Rmodel) Insupd(extra ...map[string]interface{}) error {
	p := self.ProfileTable
	fieldValues := self.getFv(p.InsertPars)
	if hasValue(extra) {
		for key, value := range extra[0] {
			if grep(p.InsertPars, key) {
				fieldValues[key] = value
			}
		}
	}
	if !hasValue(fieldValues) {
		return errors.New("pk value not found")
	}

	if err := self.insupdRest(fieldValues); err != nil {
		return err
	}

	fieldValues[self.CurrentKey] = self.LastID
	self.aLISTS = append(self.aLISTS, fieldValues)

	return nil
}

// Update updates a row using values defined in ARGS
// depending on the unique of the columns defined in UpdatePars.
// extra is for SQL constrains
func (self *Rmodel) Update(extra ...map[string]interface{}) error {
	val := self.editIdVal(extra...)
	if !hasValue(val) {
		return errors.New("pk value not found")
	}

	p := self.ProfileTable
	fieldValues := self.getFv(p.InsertPars)
	if !hasValue(fieldValues) {
		return errors.New("no data to update")
	} else if len(fieldValues) == 1 && fieldValues[self.CurrentKey] != nil {
		self.aLISTS = append(self.aLISTS, fieldValues)
		return nil
	}

	if hasValue(self.Empties) && hasValue(self.aARGS[self.Empties]) {
		if err := self.updateRest(fieldValues, val, self.aARGS[self.Empties].([]string), extra...); err != nil {
			return err
		}
	} else if err := self.updateRest(fieldValues, val, nil, extra...); err != nil {
		return err
	}

	self.aLISTS = append(self.aLISTS, fieldValues)

	return nil
}

// Delete deletes a row or multiple rows using the contraint in extra
func (self *Rmodel) Delete(extra ...map[string]interface{}) error {
	val := self.editIdVal(extra...)
	if !hasValue(val) {
		return errors.New("pk value not provided")
	}
	if err := self.deleteRest(val, extra...); err != nil {
		return err
	}

	fieldValues := make(map[string]interface{})
	if hasValue(extra) {
		for k, v := range extra[0] {
			fieldValues[k] = v
		}
	}
	fieldValues[self.CurrentKey] = self.LastID
	self.aLISTS = append(self.aLISTS, fieldValues)

	return nil
}

// properValue returns the value of key 'v' from extra.
// In case it does not exist, it tries to get from ARGS.
func (self *Rmodel) properValue(v string, extra map[string]interface{}) interface{} {
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
func (self *Rmodel) properValues(vs []string, extra map[string]interface{}) []interface{} {
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
func (self *Rmodel) properValuesHash(vs []string, extra map[string]interface{}) map[string]interface{} {
	values := self.properValues(vs, extra)
	hash := make(map[string]interface{})
	for i, v := range vs {
		hash[v] = values[i]
	}
	return hash
}
