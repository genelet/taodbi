package taodbi

import (
//"log"
	"database/sql"
	"strings"
	"fmt"
	"errors"
)

// Crud works on table's insert and select using map.
// CurrentTable: the current table name
// CurrentKey: the primary key of the table, which is always a timestamp in TDengine.
// LastID: the value woud be assigned after you insert a row.
// CurrentRow: the row corresponding to LastID
// Updated: true if the CurrentRow is old, false if it is new
type Crud struct {
	DBI          `json:"dbi,omitempty"`
	CurrentTable string                 `json:"current_table"`
	CurrentKey   string                 `json:"current_key"`
	ForeignKey   string                 `json:"foreign_key"`
	Tags         []string				`json:"tags"`
	LastID       int64                  `json:"last_id,omitempty"`
	CurrentRow   map[string]interface{} `json:"last_row,omitempty"`
	Updated      bool                   `json:"updated,omitempty"`
}

func hasValue(extra ...map[string]interface{}) bool {
	return extra != nil && len(extra) > 0

}

func selectType(select_pars interface{}) (string, []string, []string) {
	switch select_pars.(type) {
	case []string:
		labels := make([]string, 0)
		for _, v := range select_pars.([]string) {
			labels = append(labels, v)
		}
		return strings.Join(labels, ", "), labels, nil
	case map[string]string:
		labels := make([]string, 0)
		types := make([]string, 0)
		for key, val := range select_pars.(map[string]string) {
			labels = append(labels, key)
			types = append(types, val)
		}
		return strings.Join(labels, ", "), labels, types
	default:
	}
	return select_pars.(string), []string{select_pars.(string)}, nil
}

func selectCondition(extra map[string]interface{}) (string, []interface{}) {
	sql := ""
	values := make([]interface{}, 0)
	i := 0
	for field, value := range extra {
		if i > 0 {
			sql += " AND "
		}
		i++
		sql += "("

		switch value.(type) {
		case []interface{}:
			ids := value.([]interface{})
			n := len(ids)
			sql += field + " IN (" + strings.Join(strings.Split(strings.Repeat("?", n), ""), ",") + ")"
			for _, v := range ids {
				values = append(values, v)
			}
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, float32, float64:
			sql += field + " =?"
			values = append(values, value)
		case string:
			v_str := value.(string)
			if len(field) > 6 && field[(len(field)-5):len(field)] == "_gsql" {
				sql += v_str
			} else {
				sql += field + " =?"
				values = append(values, v_str)
			}
		}
		sql += ")"
	}

	return sql, values
}

func singleCondition(keyname string, id interface{}, extra ...map[string]interface{}) (string, []interface{}) {
	sql := ""
	extra_values := make([]interface{}, 0)

	switch id.(type) {
	case []interface{}:
		ids := id.([]interface{})
		n := len(ids)
		sql = "(" + keyname + " IN (" + strings.Join(strings.Split(strings.Repeat("?", n), ""), ",") + "))"
		for _, v := range ids {
			extra_values = append(extra_values, v)
		}
	default:
		sql = "(" + keyname + "=?)"
		extra_values = append(extra_values, id)
	}

	if hasValue(extra...) {
		s, arr := selectCondition(extra[0])
		sql += " AND " + s
		for _, v := range arr {
			extra_values = append(extra_values, v)
		}
	}

	return sql, extra_values
}

// InsertLast is the same as InsertHash, plus assignment to LastID, CurrentRow and Updated
func (self *Crud) InsertLast(field_values map[string]interface{}) error {
	err := self.InsertHash(field_values)
	if err != nil {
		return err
	}
	last_id := int64(0)
	err = self.Db.QueryRow(
		"SELECT LAST(" + self.CurrentKey + ") FROM " + self.CurrentTable).Scan(&last_id)
	if err != nil {
		return err
	}
	self.LastID = last_id
	field_values[self.CurrentKey] = last_id
	self.CurrentRow = field_values
	self.Updated = false
	return nil
}

// InsertHash inserts a row as map (hash) into the table. If the value of CurrenyKey
// is not given, it will be assigned as the current timestamp. 
func (self *Crud) InsertHash(field_values map[string]interface{}) error {
	sql := "INSERT INTO "
	if self.Tags != nil {
		table := ""
		using := ""
		for _, t := range self.Tags {
			v := field_values[t]
			if v==nil { return errors.New("Missing " + t) }
			table += fmt.Sprintf("_%v", v)
			using += fmt.Sprintf("%v,", Quote(v))
			delete(field_values, t)
		}
		sql += self.CurrentTable + table + " USING " + self.CurrentTable + " TAGS (" + using[:len(using)-1] + ") "
	} else {
		sql += self.CurrentTable
	}

	fields := make([]string, 0)
	values := make([]interface{}, 0)
	found := false
	for k, v := range field_values {
		if k == self.CurrentKey && v != nil {
			found = true
		}
		if v == nil {
			continue
		}
		fields = append(fields, k)
		values = append(values, v)
	}

	sql += "("
	if found == false {
		sql += self.CurrentKey + ", "
	}
	sql += strings.Join(fields, ", ") + ") VALUES ("
	if found == false {
		sql += "now,"
	}
	sql += strings.Join(strings.Split(strings.Repeat("?", len(fields)), ""), ",") + ")"
	return self.ExecSQL(sql, values...)
}

// InsupdHash inserts a new row, or retrieves the old one depending on
// wether or not the unique combinated values of the columns, 'uniques', exists. 
func (self *Crud) InsupdHash(field_values map[string]interface{}, uniques []string) error {
	s := "SELECT " + self.CurrentKey + " FROM " + self.CurrentTable + "\nWHERE "
	v := make([]interface{}, 0)
	for i, val := range uniques {
		if i > 0 {
			s += " AND "
		}
		s += val + "=?"
		v = append(v, Quote(field_values[val]))
	}

	keyvalue := int64(0)
	err := self.Db.QueryRow(s, v...).Scan(&keyvalue)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	//  err := self.QuerySQL(&lists, s, v...)

	if keyvalue == 0 {
		self.Updated = false
		return self.InsertLast(field_values)
	}

	self.Updated = true
	field_values[self.CurrentKey] = keyvalue
	self.LastID = keyvalue
	self.CurrentRow = field_values

	return nil
}

// EditHash selects rows using CurrentKey value 'ids'.
// Only will columns defined in select_pars will be returned.
// The select restriction is described in extra. See 'extra' in TopicsHash.
func (self *Crud) EditHash(lists *[]map[string]interface{}, select_pars interface{}, ids interface{}, extra ...map[string]interface{}) error {
    sql, labels, types := selectType(select_pars)
    sql = "SELECT " + sql + "\nFROM " + self.CurrentTable
    where, extra_values := singleCondition(self.CurrentKey, ids, extra...)
    if where != "" {
        sql += "\nWHERE " + where
    }

    return self.QuerySQLTypeLabel(lists, types, labels, sql, extra_values...)
}

// EditFK selects rows using one row with 'Foreign Key' value 'ids'.
// Only will columns defined in select_pars will be returned.
// The select restriction is described in extra. See 'extra' in TopicsHash.
func (self *Crud) EditHashFK(lists *[]map[string]interface{}, select_pars interface{}, ids interface{}, extra ...map[string]interface{}) error {
	sql, labels, types := selectType(select_pars)
	sql = "SELECT LAST(*) FROM " + self.CurrentTable
	where, extra_values := singleCondition(self.ForeignKey, ids, extra...)
	if where != "" {
		sql += "\nWHERE " + where
	}

	return self.QuerySQLTypeLabel(lists, types, labels, sql, extra_values...)
}

// TopicsHash selects rows using restriction 'extra'.
// Only will columns defined in select_pars will be returned.
// Currently only will the following three tpyes os restrictions are support:
// key=>value: the key has the value
// key=>slice: the key has one of the values in the slice
// '_gsql'=>'raw sql statement': use the raw SQL statement
func (self *Crud) TopicsHash(lists *[]map[string]interface{}, select_pars interface{}, extra ...map[string]interface{}) error {
	return self.TopicsHashOrder(lists, select_pars, "", extra...)
}

// TopicsHashOrder is the same as TopisHash, but use the order string as 'ORDER BY order'
func (self *Crud) TopicsHashOrder(lists *[]map[string]interface{}, select_pars interface{}, order string, extra ...map[string]interface{}) error {
	sql, _, types := selectType(select_pars)
	sql = "SELECT " + sql + "\nFROM " + self.CurrentTable

	if hasValue(extra...) {
		where, values := selectCondition(extra[0])
		sql += "\nWHERE " + where
		if order != "" {
			sql += "\nORDER BY " + order
		}
		return self.QuerySQLType(lists, types, sql, values...)
	}

	if order != "" {
		sql += "\nORDER BY " + order
	}
	return self.QuerySQLType(lists, types, sql)
}

// TotalHash returns the total number of rows available
// This function is most likely be used for pagination.
func (self *Crud) TotalHash(v interface{}, extra ...map[string]interface{}) error {
	str := "SELECT COUNT(*) FROM " + self.CurrentTable

	var err error
	if hasValue(extra...) {
		where, values := selectCondition(extra[0])
		if where != "" {
			str += "\nWHERE " + where
		}
		err = self.Db.QueryRow(str, Quotes(values)...).Scan(v)
	}

	err = self.Db.QueryRow(str).Scan(v)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}
