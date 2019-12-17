package taodbi

import (
	"fmt"
	"errors"
	"encoding/json"
)

type Smodel struct {
	Model

	CurrentStable	string	`json:"current_stable"`
	Tags		[]string	`json:"tags"`
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
	str :=`SELECT LAST(*) FROM `+self.CurrentStable+` WHERE `+self.ForeignKey+`=?`
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

func (self *Smodel)Stopics(extra ...map[string]interface{}) error {
	str, values := self.getSQL(extra...)
	str += " GROUP BY "
	for _, tag := range self.Tags {
		str += "?,"
		values = append(values, self.ARGS[tag])
	}
	str = str[:len(str)-1]

    self.LISTS = []map[string]interface{}{}
	return self.SelectSQLLabel(&self.LISTS, append(self.TopicsPars, self.Tags...), str, values...)
}

func (self *Smodel)Sedit(extra ...map[string]interface{}) error {
	str, values := self.getSQL(extra...)
	for _, tag := range self.Tags {
		str += " AND " + tag + "=?"
		values = append(values, self.ARGS[tag])
	}

    self.LISTS = []map[string]interface{}{}
	return self.SelectSQLLabel(&self.LISTS, append(self.EditPars, self.Tags...), str, values...)
}

/*
func (self *Smodel) InsertTag(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil { one = extra[0] }
	tag_value := self.ProperValue(self.TAGNAME, one)
	type_value := self.ProperValue(self.TYPENAME, one)
	if tag_value==nil || type_value==nil {
		return errors.New("Missing tag or type")
	}
	return self.ExecSQL(
`ALTER TABLE ` +self.CurrentStable+ ` ADD TAG ? ?`, tag_value, type_value)
}

func (self *Smodel) DeleteTag(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil { one = extra[0] }
	tag_value := self.ProperValue(self.TAGNAME, one)
	if tag_value==nil {
		return errors.New("Missing tag")
	}
	return self.ExecSQL(
`ALTER TABLE ` +self.CurrentStable+ ` DROP TAG ?`, tag_value)
}
*/

func (self *Smodel)MakeTableUsing(extra ...map[string]interface{}) error {
	var one map[string]interface{}
	if extra != nil { one = extra[0] }
	values := self.ProperValues(self.Tags, one)
	table :=  self.CurrentStable
	using := "USING "+self.CurrentStable+" TAGS ("
	for i, v := range values {
		if v==nil { return errors.New("Missing " + self.Tags[i]) }
		table += fmt.Sprintf("_%v", v)
		using += fmt.Sprintf("%v,", Quote(v))
	}
	using = using[:len(using)-1] + ")"
	self.CurrentTable = table
	self.UsingTags = using
	return nil
}

func (self *Smodel) CreateTable(extra ...map[string]interface{}) error {
	if err := self.MakeTableUsing(extra...); err != nil {
		return err
	}
	return self.ExecSQL("CREATE TABLE IF NOT EXISTS " + self.CurrentTable + " " + self.UsingTags)
}
