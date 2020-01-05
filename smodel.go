package taodbi

import (
"log"
	"fmt"
	"errors"
	"encoding/json"
)

type Smodel struct {
	Model
}

// NewSmodel constructs a new Model object from the json model string
func NewSmodel(content []byte) (*Smodel, error) {
    parsed := new(Smodel)
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

func (self *Smodel) getSQL(extra ...map[string]interface{}) (string, []interface{}) {
	str := `SELECT LAST(*) FROM ` + self.CurrentTable + ` WHERE ` + self.ForeignKey + `=?`
	values := []interface{}{self.ARGS[self.ForeignKey]}
	if hasValue(extra...) {
        s, arr := selectCondition(extra[0])
        str += " AND " + s
        for _, v := range arr {
            values = append(values, v)
        }
    }
	return str, values
}

// LastTopics reports items of a given foreign key in all tables under a super table.
func (self *Smodel) LastTopics(extra ...map[string]interface{}) error {
	if self.ForeignKey == "" {
		return errors.New("no foreign key")
	}
	if _, ok := self.ARGS[self.ForeignKey]; !ok {
		return errors.New("no value for foreign key " + self.ForeignKey)
	}
	str, values := self.getSQL(extra...)
	str += " GROUP BY "
	for _, tag := range self.Tags {
		str += tag + ","
	}
	str = str[:len(str)-1]

    self.LISTS = []map[string]interface{}{}
	return self.SelectSQLLabel(&self.LISTS, self.TopicsPars, str, values...)
}

// ReleaseTopics reports all items in the latest release which is represented
// as the first tag with type int (unix epoch time) in the super table
func (self *Smodel) ReleaseTopics(extra ...map[string]interface{}) error {
	ts := 0
	release := 0
	err := self.Db.QueryRow(
`SELECT LAST(` + self.CurrentKey + `)
FROM ` + self.CurrentTable + `
GROUP BY ` + self.Tags[0] + `
ORDER BY ` + self.Tags[0] + ` DESC LIMIT 1`).Scan(&ts, &release)
	if err != nil { return err }

	if hasValue(extra...) {
		extra[0][self.Tags[0]] = release
	} else {
		extra = []map[string]interface{}{{self.Tags[0]:release}}
	}

	return self.Topics(extra...)
}

func indexString(vs []string, t string) int {
    for i, v := range vs {
        if v == t { return i }
    }
    return -1
}

// LastEdit reports one item of a given foreign key in super table.
func (self *Smodel)LastEdit(extra ...map[string]interface{}) error {
	if self.ForeignKey == "" { return errors.New("no foreign key") }
	str, values := self.getSQL(extra...)
	for _, tag := range self.Tags {
		str += " AND " + tag + "=?"
		values = append(values, self.ARGS[tag])
	}

    self.LISTS = []map[string]interface{}{}
	labels := make([]string,0)
	for _, label := range self.EditPars {
		if indexString(self.Tags, label)<0 {
			labels = append(labels, label)
		}
	}
	return self.SelectSQLLabel(&self.LISTS, labels, str, values...)
}

// CreateTable create a table using tags and current super table
func (self *Smodel) CreateTable(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if hasValue(extra...) { one = extra[0] }
	values := self.ProperValues(self.Tags, one)
	table :=  self.CurrentTable
	using := "USING "+self.CurrentTable+" TAGS ("
	for i, v := range values {
		if v==nil { return errors.New("Missing " + self.Tags[i]) }
		table += fmt.Sprintf("_%v", v)
		using += fmt.Sprintf("%v,", Quote(v))
	}
	using = using[:len(using)-1] + ")"
log.Printf("%s", "CREATE TABLE IF NOT EXISTS " + table + " " + using)
	return self.ExecSQL("CREATE TABLE IF NOT EXISTS " + table + " " + using)
}

// DropTable drops a table using tags and current super table
func (self *Smodel) DropTable(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil { one = extra[0] }
	values := self.ProperValues(self.Tags, one)
	table :=  self.CurrentTable
	for i, v := range values {
		if v==nil { return errors.New("Missing " + self.Tags[i]) }
		table += fmt.Sprintf("_%v", v)
	}
	return self.ExecSQL("DROP TABLE IF EXISTS " + table)
}
