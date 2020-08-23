package taodbi

import (
	"fmt"
	"errors"
	"encoding/json"
	"strings"
	"io/ioutil"
)

type Smodel struct {
	Model
	Tags          []string  `json:"tags,omitempty"`
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
	parsed.acrud = parsed

    return parsed, nil
}

func (self *Smodel) insertExtra(args map[string]interface{}) string {
	table := ""
	using := ""
	for _, t := range self.Tags {
		v, ok := args[t]
		if !ok {
			return ""
		}
		switch u := v.(type) {
		case int:
			table += fmt.Sprintf("_%d", u)
			using += Quote(fmt.Sprintf("%d", u)).(string) + ","
		default:
			table += "_" + v.(string)
			using += Quote(v).(string) + ","
		}
		delete(args, t)
	}

    return table + " USING " + self.CurrentTable + " TAGS (" + using[:len(using)-1] + ") "
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
	sql = `SELECT LAST(*) FROM ` + self.CurrentTable
	where, values := singleCondition(self.ForeignKey, val, extra...)
	sql += ` WHERE ` + where + " GROUP BY " + strings.Join(self.Tags, ",")

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
