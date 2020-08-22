package taodbi

import (
//"log"
	"fmt"
	"errors"
	"encoding/json"
	"strings"
	"io/ioutil"
	"math"
	"regexp"
)

type Smodel struct {
	Model
}

// NewSmodel creates a new Rmodel struct from json file 'filename'
// You should use SetDB to assign a database handle and
// SetArgs to set input data, a url.Value, to make it working
//
func NewSmodel(filename string) (*Smodel, error) {
    content, err := ioutil.ReadFile(filename)
    if err != nil { return nil, err }
    var parsed *Smodel
    if err := json.Unmarshal(content, &parsed); err != nil {
        return nil, err
    }
    parsed.Crud.fulfill()

    return parsed, nil
}

// Topics selects many rows, optionally with restriction defined in 'extra'.
func (self *Smodel) Topics(extra ...map[string]interface{}) error {
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
func (self *Smodel) orderString() string {
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
func (self *Smodel) Edit(extra ...map[string]interface{}) error {
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
func (self *Smodel) EditFK(extra ...map[string]interface{}) error {
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
func (self *Smodel) Insert(extra ...map[string]interface{}) error {
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
func (self *Smodel) Insupd(extra ...map[string]interface{}) error {
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

// LastTopics reports items of a given foreign key in all tables under a super table.
func (self *Smodel) LastTopics(extra ...map[string]interface{}) error {
    val := self.editFKVal(extra...)
    if !hasValue(val) {
        return errors.New("fk value not provided")
    }

	hashPars := self.topicsHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(self.TopicsHash, self.TopicsPars, fields.([]string))
    }
	sql, labels, types := selectType(hashPars)
	sql = `SELECT LAST(*)\nFROM ` + self.CurrentTable
	where, values := singleCondition(self.ForeignKey, val, extra...)
	sql += `\nWHERE ` + where + "\nGROUP BY " + strings.Join(self.Tags, ",")

	self.aLISTS = make([]map[string]interface{}, 0)
	return self.SelectSQLTypeLabel(&self.aLISTS, types, labels, sql, values...)
}

// LastEdit reports one item of a given foreign key in super table.
// it may be replaced by EditFK by putting tags' values in extra
func (self *Smodel)LastEdit(extra ...map[string]interface{}) error {
	if !hasValue(extra) {
		extra = []map[string]interface{}{make(map[string]interface{})}
	}
	for _, tag := range self.Tags {
		if _, ok1 := extra[0][tag]; !ok1 {
			if v, ok2 := self.aARGS[tag]; ok2 {
				extra[0][tag] = v
			}
		}
	}

    val := self.editFKVal(extra...)
    if !hasValue(val) {
        return errors.New("fk value not provided")
    }

    hashPars := self.editHashPars
    if fields, ok := self.aARGS[self.Fields]; ok {
        hashPars = generalHashPars(self.EditHash, self.EditPars, fields.([]string))
    }

    self.aLISTS = make([]map[string]interface{}, 0)
    return self.editHashFK(&self.aLISTS, hashPars, val, extra...)
}

// ReleaseTopics reports all items in the latest release which is represented
// as the first tag with type int (unix epoch time) in the super table
func (self *Smodel) ReleaseTopics(extra ...map[string]interface{}) error {
	rtag := self.Tags[0]
	ts := 0
	release := 0
	err := self.DB.QueryRow(
`SELECT LAST(` + self.CurrentKey + `)
FROM ` + self.CurrentTable + `
GROUP BY ` + rtag + `
ORDER BY ` + rtag + ` DESC LIMIT 1`).Scan(&ts, &release)
	if err != nil { return err }

	if hasValue(extra) {
		extra[0][rtag] = release
	} else {
		extra = []map[string]interface{}{{rtag:release}}
	}

	return self.Topics(extra...)
}

// CreateTable create a table using tags and current super table
func (self *Smodel) CreateTable(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if hasValue(extra) {
		one = extra[0]
	}
	values := self.properValues(self.Tags, one)
	table :=  self.CurrentTable
	using := "USING "+self.CurrentTable+" TAGS ("
	for i, v := range values {
		if v==nil { return errors.New("Missing " + self.Tags[i]) }
		table += fmt.Sprintf("_%v", v)
		using += fmt.Sprintf("%v,", Quote(v))
	}
	using = using[:len(using)-1] + ")"
	return self.DoSQL("CREATE TABLE IF NOT EXISTS " + table + " " + using)
}

// DropTable drops a table using tags and current super table
func (self *Smodel) DropTable(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil {
		one = extra[0]
	}
	values := self.properValues(self.Tags, one)
	table :=  self.CurrentTable
	for i, v := range values {
		if v==nil { return errors.New("Missing " + self.Tags[i]) }
		table += fmt.Sprintf("_%v", v)
	}
	return self.DoSQL("DROP TABLE IF EXISTS " + table)
}
