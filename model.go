package taodbi

import (
	"database/sql"
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
