package taodbi

import (
	"errors"
	"io/ioutil"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type Rmodel struct {
	Model

	ProfileTable *crud    `json:"profile_table,omitempty"` // non unique fields
	StatusTable  *crud    `json:"status_table,omitempty"`  // gmark_delete
}

// NewRmodel creates a new Rmodel struct from json file 'filename'
// You should use SetDB to assign a database handle and
// SetArgs to set input data, a url.Value, to make it working
//
func NewRmodel(filename string) (*Rmodel, error) {
	content, err := ioutil.ReadFile(filename)
    if err != nil { return nil, err }
    var parsed *Rmodel
	if err := json.Unmarshal(content, &parsed); err != nil {
        return nil, err
    }
    parsed.crud.fulfill()
    parsed.ProfileTable.fulfill()
    parsed.StatusTable.fulfill()

	parsed.acrud = parsed
	parsed.ProfileTable.acrud = parsed.ProfileTable
	parsed.StatusTable.acrud = parsed.StatusTable

    return parsed, nil
}

// SetDB sets the DB handle
func (self *Rmodel) SetDB(db *sql.DB) {
    self.Model.SetDB(db)
    self.ProfileTable.DB = db
    self.StatusTable.DB = db
}

func (self *Rmodel) getStatus(id interface{}) (bool, error) {
	s := self.StatusTable
	status := false
	err := self.DB.QueryRow("SELECT LAST("+s.statusColumn()+") FROM "+s.CurrentTable+" WHERE "+s.ForeignKey+"=?", id).Scan(&status)
	return status, err
}

// insertRest inserts one row into each table.
// args: the input row data expressed as url.Values.
// The keys are column names, and their values are columns' values.
//
func (self *Rmodel) insertRest(args map[string]interface{}) error {
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
func (self *Rmodel) updateRest(args map[string]interface{}, ids []interface{}, empties []string, extra ...map[string]interface{}) error {
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
func (self *Rmodel) deleteRest(ids []interface{}, extra ...map[string]interface{}) error {
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
func (self *Rmodel) insupdRest(args map[string]interface{}) error {
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
func (self *Rmodel) editRest(lists *[]map[string]interface{}, editPars interface{}, ids []interface{}, extra ...map[string]interface{}) error {
	p := self.ProfileTable
	return p.editHashFK(lists, editPars, ids, extra...)
}

func (self *Rmodel) getPlainLists(passid interface{}, rowcount int, reverse bool) ([]interface{}, error) {
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
func (self *Rmodel) topicsRest(rowcount int, reverse bool, passid interface{}, lists *[]map[string]interface{}, selectPars interface{}, extra ...map[string]interface{}) error {
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
func (self *Rmodel) totalRest(start, end, v, n *int64) error {
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


// ...................
//
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
