package taodbi

import (
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
	parsed, err := newRest(filename)
    if err != nil { return nil, err }
	if db!=nil {
		parsed.Crud.DB = db
		parsed.ProfileTable.DB = db
		parsed.StatusTable.DB = db
	}

    return parsed, nil
}

func newRest(filename string) (*Restful, error) {
	content, err := ioutil.ReadFile(filename)
    if err != nil { return nil, err }
    var parsed *Restful
	if err := json.Unmarshal(content, &parsed); err != nil {
        return nil, err
    }
    parsed.Crud.fulfill()
    parsed.ProfileTable.fulfill()
    parsed.StatusTable.fulfill()

    return parsed, nil
}

func (self *Restful) getStatus(id interface{}) (bool, error) {
	s := self.StatusTable
	status := false
	err := self.DB.QueryRow("SELECT LAST("+s.statusColumn()+") FROM "+s.CurrentTable+" WHERE "+s.ForeignKey+"=?", id).Scan(&status)
	return status, err
}

// insertRest inserts one row into each table.
// args: the input row data expressed as url.Values.
// The keys are column names, and their values are columns' values.
//
func (self *Restful) insertRest(args map[string]interface{}) error {
	var id interface{}
	extra := make(map[string]interface{})

	if hasValue(self.InsertPars) {
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
		if len(lists)>0 {
			id = lists[0][self.CurrentKey]
			if status, err := self.getStatus(id); err != nil {
				return err
			} else if status {
				return errors.New("current unique key already taken")
			}
			self.LastID = id.(int64)
		} else if err := self.insertHash(extra); err != nil {
			return err
		} else {
			id = self.LastID
		}
	} else {
		extra["useless"] = false
		if err := self.insertHash(extra); err != nil {
			return err
		} else {
			id = self.LastID
		}
	}

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
		status, err := self.getStatus(id)
		if err == nil && !status {
			err = self.DoSQL("INSERT INTO "+self.StatusTable.CurrentTable+" VALUES (now, ?, true)", id)
		}
		if err != nil {
			return err
		}

        args[p.ForeignKey] = id
		// unlike insertRest, columns without values will NOT be filled with the last ones
		return p.insertHash(args)
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

func (self *Restful) getPlainLists(passid interface{}, rowcount int, reverse bool) ([]interface{}, error) {
	gsql := self.CurrentKey
	order := "ORDER BY " + self.CurrentKey
	if reverse {
		gsql += "<"
		order += " DESC "
	} else {
		gsql += ">"
	}
	gsql += fmt.Sprintf("%d", passid)
	order += " LIMIT " + fmt.Sprintf("%d", rowcount)
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
func (self *Restful) topicsRest(rowcount int, reverse bool, passid interface{}, lists *[]map[string]interface{}, selectPars interface{}, extra ...map[string]interface{}) error {
	if rowcount < 1 {
		return errors.New("no row counts")
	}
	countTable := 0
	if err := self.totalHash(&countTable); err != nil {
		return err
	}

	ignore := passid
	count := 0
	total := 0
	for {
		ids, err := self.getPlainLists(ignore, rowcount, reverse)
		if err != nil { return err }
		nMain := len(ids)
		if nMain == 0 { return nil } // no record left in main
		count += nMain
		ignore = ids[nMain-1]

		outs := make([]interface{}, 0)
		for _, id := range ids {
			if status, err := self.getStatus(id); err != nil {
				return err
			} else if status {
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
			if total >= rowcount { // rowcount of records found
				return nil
			}
		}

		count += nMain
		if count >= countTable { return nil }
	}

	return nil
}

// totalRest returns the start, end and total number of rows available
// This function is used for pagination.
// extra: optional, extra constraints on WHERE statement.
//
func (self *Restful) totalRest(start, end, v, n *int64) error {
	query := "SELECT "+self.CurrentKey+" FROM " + self.CurrentTable
	sth, err := self.DB.Prepare(query)
	if err != nil {
        return err
    }
	defer sth.Close()

	s := self.StatusTable
	sta, err := self.DB.Prepare("SELECT LAST("+s.statusColumn()+") FROM "+s.CurrentTable+" WHERE "+s.ForeignKey+"=?")
	if err != nil {
        return err
    }
	defer sth.Close()

	rows, err := sth.Query()
    if err != nil {
        return err
    }
	defer rows.Close()

	status := false
	id := int64(0)
	i  := int64(0)
	raw:= int64(0)
	for rows.Next() {
		raw++
		if err = rows.Scan(&id); err != nil {
            return err
        }
		if err := sta.QueryRow(id).Scan(&status); err != nil {
			return err
		}
		if status {
			if i==0 {
				*start = id
			}
			i++
		}
	}
    if err := rows.Err(); err != nil && err != sql.ErrNoRows {
        return err
    }

	*end = id
	*v = i
	*n = raw
	return nil
}

