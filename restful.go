package taodbi

import (
//"log"
	"errors"
	"io/ioutil"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Restful struct {
	Crud

	ProfileTable *Crud    `json:"profile_table,omitempty"` // non unique fields
	StatusTable  *Crud    `json:"status_table,omitempty"`  // gmark_delete
	Updated      bool     `json:"-"`                       // for main
}

func newRestful(db *sql.DB, filename string) (*Restful, error) {
	content, err := ioutil.ReadFile(filename)
    if err != nil { return nil, err }
    var parsed *Restful
	if err := json.Unmarshal(content, &parsed); err != nil {
        return nil, err
    }
    parsed.Crud.fulfill()
    parsed.Crud.DB = db
    parsed.ProfileTable.fulfill()
    parsed.ProfileTable.DB = db
    parsed.StatusTable.fulfill()
    parsed.StatusTable.DB = db

    return parsed, nil
}

// insertRest inserts one row into each table.
// args: the input row data expressed as url.Values.
// The keys are column names, and their values are columns' values.
//
func (self *Restful) insertRest(args map[string]interface{}) error {
	extra := make(map[string]interface{})
	for _, k := range self.InsertPars {
		v, ok := args[k]
		if !ok {
			return errors.New("missing unique key: " + k)
		}
		extra[k] = v
	}
	if err := self.insertHash(extra); err != nil {
		return err
	}

	id := self.LastID

	p := self.ProfileTable
	extra = make(map[string]interface{})
	for _, k := range p.InsertPars {
		if v, ok := args[k]; ok {
			extra[k] = v
		}
	}
	extra[p.ForeignKey] = id
	if err := p.insertHash(extra); err != nil {
		return err
	}

	return self.DoSQL("INSERT INTO " + self.StatusTable.CurrentTable + " VALUES (now, ?, true)", id)
}

// updateRest updates multiple rows using data expressed in type Values.
// args: columns names and their new values.
// ids: FK's value, either a single value or array of values.
// extra: optional, extra constraints put on row's WHERE statement.
//
func (self *Restful) updateRest(args map[string]interface{}, ids []interface{}, empties []string, extra ...map[string]interface{}) error {
	lists := make([]map[string]interface{}, 0)
	p := self.ProfileTable
	if err := p.editHashFK(&lists, p.InsertPars, ids, extra...); err != nil {
		return err
	}
	for _, item := range lists {
		hash := make(map[string]interface{})
		for _, k := range p.InsertPars {
			if v, ok := args[k]; ok {
				hash[k] = v
			} else if hasValue(empties) && grep(empties, k) {
				continue
			} else if v, ok := item[k]; ok {
				hash[k] = v
			}
		}
		if err := p.insertHash(hash); err != nil {
			return err
		}
	}

	return nil
}

// deleteRest deletes rows by extra: constraints on WHERE statement.
//
func (self *Restful) deleteRest(ids []interface{}, extra ...map[string]interface{}) error {
	lists := make([]map[string]interface{}, 0)
	p := self.ProfileTable
	if err := p.editHashFK(&lists, p.ForeignKey, ids, extra...); err != nil {
		return err
	}
	s := self.StatusTable
	for _, item := range lists {
		if err := self.DoSQL("INSERT INTO " + s.CurrentTable + " VALUES (now, ?, false)", item[p.ForeignKey]); err != nil {
			return err
		}
	}
	return nil
}

// insupdRest updates if it is found to exists, or to inserts a new record
// args: row's column names and values
//
func (self *Restful) insupdRest(args map[string]interface{}) error {
	extra := make(map[string]interface{})
	for _, k := range self.InsertPars {
		v, ok := args[k]
		if !ok {
			return errors.New("missing unique key: " + k)
		}
		extra[k] = v
	}
	lists := make([]map[string]interface{}, 0)
	if err := self.topicsHash(&lists, self.CurrentKey, "", extra); err != nil {
		return err
	}

	p := self.ProfileTable
	if len(lists) > 1 {
		return errors.New("multiple returns for unique key")
	} else if len(lists) == 1 {
		self.Updated = true
		id := lists[0][self.CurrentKey]
        args[p.ForeignKey] = id
		if err := self.updateRest(args, []interface{}{id}, nil); err != nil {
			return err
		}

		s := self.StatusTable
		status := false
		err := self.DB.QueryRow("SELECT LAST("+s.statusColumn()+") FROM "+s.CurrentTable+" WHERE "+s.ForeignKey+"=?", id).Scan(&status)
		if err == nil && !status {
			return self.DoSQL("INSERT INTO "+s.CurrentTable+" VALUES (now, ?, true)", id)
		}
		return err
	}

	self.Updated = false
	return self.insertRest(args)
}

// editRest selects rows using PK ids, constrained by extra
// Only will columns defined in select_pars will be returned.
//
func (self *Restful) editRest(lists *[]map[string]interface{}, editPars interface{}, ids []interface{}, extra ...map[string]interface{}) error {
	p := self.ProfileTable
	return p.editHashFK(lists, editPars, ids, extra...)
}

func (self *Restful) getPlainLists(lastID interface{}, rowcount int, reverse bool) ([]interface{}, error) {
	gsql := self.CurrentKey
	order := "ORDER BY " + self.CurrentKey
	if reverse {
		gsql += ">"
		order += " DESC "
	} else {
		gsql += "<"
	}
	gsql += fmt.Sprintf("%d", lastID)
	order += "LIMIT " + fmt.Sprintf("%d", rowcount)
	lists := make([]map[string]interface{},0)
	if err := self.topicsHash(&lists, self.CurrentKey, order, map[string]interface{}{"_gsql":gsql}); err != nil {
		return nil, err
	}
	ids := make([]interface{}, 0)
	for _, item := range lists {
		ids = append(ids, item[self.CurrentKey])
	}

	return ids, nil
}

// topicsRest selects rows by pages
// lists: received the query results in slice of maps.
// extra: optional, extra constraints on WHERE statement.
// topicsPars: defining which and how columns are returned:
// 1) []string{name} - name of column
// 2) [2]string{name, type} - name and data type of column
// 3) map[string]string{name: label} - column name is mapped to label
// 4) map[string][2]string{name: label, type} -- column name to label and data type
//
func (self *Restful) topicsRest(rowcount int, reverse bool, lastID interface{}, lists *[]map[string]interface{}, selectPars interface{}, extra ...map[string]interface{}) error {
	if rowcount < 1 {
		return errors.New("no row counts")
	}
	countTable := 0
	if err := self.totalHash(&countTable); err != nil {
		return err
	}

	count := 0
	total := 0
	for {
		ids, err := self.getPlainLists(lastID, rowcount, reverse)
		if err != nil { return err }
		nMain := len(ids)
		if nMain == 0 { return nil }
		count += nMain
		lastID = ids[nMain-1]

		s := self.StatusTable
		c := s.statusColumn()
		outs := make([]interface{}, 0)
		for _, id := range ids {
			status := false
			if err := self.DB.QueryRow("SELECT LAST("+c+") FROM "+s.CurrentTable+" WHERE "+s.ForeignKey+"=?", id).Scan(&status); err != nil {
				return err
			}
			if status {
				outs = append(outs, id)
			}
		}
		if len(outs)<1 {
			continue
		}

		p := self.ProfileTable
		items := make([]map[string]interface{}, 0)
		if err := p.editHashFK(&items, selectPars, outs, extra...); err != nil {
			return err
		}
		for _, item := range items {
			*lists = append(*lists, item)
			total++
			if total >= rowcount {
				return nil
			}
		}

		if nMain < rowcount { return nil }
		count += nMain
		if count >= countTable { return nil }
	}

	return nil
}

// totalRest returns the total number of rows available
// This function is used for pagination.
// v: the total number is returned in this referenced variable
// extra: optional, extra constraints on WHERE statement.
//
/*
func (self *Restful) totalRest(v interface{}, extra ...url.Values) error {
	str := "SELECT COUNT(*) FROM " + self.CurrentTable

	if hasValue(extra) {
		where, values := selectCondition(extra[0])
		if where != "" {
			str += "\nWHERE " + where
		}
		return self.DB.QueryRow(str, values...).Scan(v)
	}

	return self.DB.QueryRow(str).Scan(v)
}
*/

