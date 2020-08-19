package taodbi

import (
	"errors"
	"strings"
)

// Crud works on one table's CRUD or RESTful operations:
// C: create a new row
// R: read all rows, or read one row
// U: update a row
// D: delete a row
//
type Crud struct {
	DBI
	Table
}

// insertHash inserts one row into the table.
// args: the input row data expressed as map[string]interface{}.
// The keys are column names, and their values are columns' values.
//
func (self *Crud) insertHash(args map[string]interface{}) error {
    sql := "INSERT INTO " + self.CurrentTable
    if self.Tags != nil {
        table := ""
        using := ""
        for _, t := range self.Tags {
            v, ok := args[t]
            if !ok {
                return errors.New("missing tag: " + t)
            }
            table += "_" + v.(string)
            using += Quote(v).(string) + ","
            delete(args, t)
        }
        sql += table + " USING " + self.CurrentTable + " TAGS (" + using[:len(using)-1] + ") "
    }

    fields := make([]string, 0)
    values := make([]interface{}, 0)
    found := false
    for k, v := range args {
        if k == self.CurrentKey {
            found = true
        }
        fields = append(fields, k)
        values = append(values, v)
    }
    sql += " ("
    if found==false {
        sql += self.CurrentKey + ", "
    }
    sql += strings.Join(fields, ", ") + ") VALUES ("
    if found==false {
        sql += "now,"
    }
    sql += strings.Join(strings.Split(strings.Repeat("?", len(fields)), ""), ",") + ")"
    if err := self.DoSQL(sql, values...); err != nil {
		return err
	}
    id := self.LastID
    if id == 0 {
        if err := self.DB.QueryRow("SELECT LAST(" + self.CurrentKey + ") FROM " + self.CurrentTable).Scan(&id); err != nil {
            return err
        }
        self.LastID = id
    }
	return nil
}

// editHash selects one or multiple rows from the primary key.
// lists: received the query results in slice of maps.
// ids: primary key values array.
// extra: optional, extra constraints on WHERE statement.
// editPars: defining which and how columns are returned:
// 1) []string{name} - name of column
// 2) [2]string{name, type} - name and data type of column
// 3) map[string]string{name: label} - column name is mapped to label
// 4) map[string][2]string{name: label, type} -- column name to label and data type
//
func (self *Crud) editHash(lists *[]map[string]interface{}, editPars interface{}, ids []interface{}, extra ...map[string]interface{}) error {
	sql, labels, types := selectType(editPars)
	sql = "SELECT " + sql + "\nFROM " + self.CurrentTable
	where, extraValues := singleCondition(self.CurrentKey, ids, extra...)
	if where != "" {
		sql += "\nWHERE " + where
	}

	return self.SelectSQLTypeLabel(lists, types, labels, sql, extraValues...)
}

// filterExtra returns only extra filtered by keys in 'keys'
//
func filterExtra(keys []string, extra map[string]interface{}) map[string]interface{} {
    if !hasValue(extra) {
        return nil
    }

    extraNew := make(map[string]interface{})
    for _, k := range keys {
        if v, ok := extra[k]; ok {
            extraNew[k] = v
        }
    }
    return extraNew
}

func (self *Crud) statusColumn() string {
    for _, column := range self.InsertPars {
        if column != self.ForeignKey {
            return column
        }
    }
    return ""
}

// editHashFK selects LAST (one or multiple) rows from the foreign keys.
// lists: received the query results in slice of maps.
// ids: foreign key values array.
// extra: optional, extra constraints on WHERE statement.
// editPars: defining which and how columns are returned:
// 1) []string{name} - name of column
// 2) [2]string{name, type} - name and data type of column
// 3) map[string]string{name: label} - column name is mapped to label
// 4) map[string][2]string{name: label, type} -- column name to label and data type
//
func (self *Crud) editHashFK(lists *[]map[string]interface{}, editPars interface{}, ids []interface{}, extra ...map[string]interface{}) error {
    sql, labels, types := selectType(editPars)
	for _, id := range ids {
	    where, extraValues := singleCondition(self.ForeignKey, []interface{}{id}, extra...)
		res := make(map[string]interface{})
		query := "SELECT LAST("+self.CurrentKey+")\nFROM "+self.CurrentTable+"\nWHERE "+where
		if err := self.GetSQLLabel(res, query, []string{self.CurrentKey}, extraValues...); err != nil {
			return err
		}
		ts, ok := res[self.CurrentKey]
		if !ok { continue }
		items := make([]map[string]interface{}, 0)
		query = "SELECT "+sql+"\nFROM "+self.CurrentTable+"\nWHERE "+self.CurrentKey+"=?"
		if err := self.SelectSQLTypeLabel(&items, types, labels, query, ts); err != nil {
			return err
		}
		*lists = append(*lists, items...)
	}
	return nil
}

// topicsHash selects all rows.
// lists: received the query results in slice of maps.
// extra: optional, extra constraints on WHERE statement.
// order: a string like 'ORDER BY ...'
// topicsPars: defining which and how columns are returned:
// 1) []string{name} - name of column
// 2) [2]string{name, type} - name and data type of column
// 3) map[string]string{name: label} - column name is mapped to label
// 4) map[string][2]string{name: label, type} -- column name to label and data type
//
func (self *Crud) topicsHash(lists *[]map[string]interface{}, selectPars interface{}, order string, extra ...map[string]interface{}) error {
	sql, labels, types := selectType(selectPars)
	sql = "SELECT " + sql + "\nFROM " + self.CurrentTable

	if hasValue(extra) {
		where, values := selectCondition(extra[0])
		if where != "" {
			sql += "\nWHERE " + where
		}
		if order != "" {
			sql += "\n" + order
		}
		return self.SelectSQLTypeLabel(lists, types, labels, sql, values...)
	}

	if order != "" {
		sql += "\n" + order
	}
	return self.SelectSQLTypeLabel(lists, types, labels, sql)
}

// totalHash returns the total number of rows available
// This function is used for pagination.
// v: the total number is returned in this referenced variable
// extra: optional, extra constraints on WHERE statement.
//
func (self *Crud) totalHash(v interface{}, extra ...map[string]interface{}) error {
	str := "SELECT COUNT(*) FROM\n" + self.CurrentTable

	if hasValue(extra) {
		where, values := selectCondition(extra[0])
		if where != "" {
			str += "\nWHERE " + where
		}
		return self.DB.QueryRow(str, values...).Scan(v)
	}

	return self.DB.QueryRow(str).Scan(v)
}
