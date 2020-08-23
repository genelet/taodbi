package taodbi

import (
	"database/sql"
	"errors"
	"net/url"
	"strings"
)

// Quote escapes string to be used safely in placeholder.
// The SQL functions in the package have already quoted so
// you should not call this again in using them.
func Quote(v interface{}) interface{} {
	switch v.(type) {
	case string:
		str := v.(string)
		str = strings.Trim(str, `'"`)
		str = strings.Replace(str, `'`, `\'`, -1)
		str = strings.Replace(str, `;`, `\;`, -1)
		return `'` + str + `'`
	default:
		return v
	}
	return v
}

// Quotes quote a slice of values for use in placeholders
func Quotes(args []interface{}) []interface{} {
	if !hasValue(args) {
		return nil
	}
	newArgs := make([]interface{}, 0)
	for _, v := range args {
		newArgs = append(newArgs, Quote(v))
	}
	return newArgs
}

// DBI simply embeds GO's generic SQL handler.
// It adds a set of functions for easier database executions and queries.
//
type DBI struct {
	// Embedding the generic database handle.
	*sql.DB `json:"-"`
	// LastID: the last auto id inserted, if the database provides
	LastID int64 `json:"-"`
	// Affected: the number of rows affected
	Affected int64 `json:"-"`
}

// DoSQL is the same as SQL's Exec, except for using a prepared statement,
// which is safe for concurrent use by multiple goroutines.
//
func (self *DBI) DoSQL(query string, args ...interface{}) error {
	//glog.Infof("godbi SQL statement: %s", query)
	//glog.Infof("godbi input data: %v", args)

	sth, err := self.DB.Prepare(query)
	if err != nil {
		return err
	}
	res, err := sth.Exec(Quotes(args)...)
	if err != nil {
		return err
	}

	/*
	   LastID, err := res.LastInsertId()
	   if err != nil {
	       return err
	   }
	   self.LastID = LastID
	*/
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	self.Affected = affected

	sth.Close()
	return nil
}

// DoSQLs inserts multiple rows at once.
// Each row is represented as array and the rows are array of array.
//
func (self *DBI) DoSQLs(query string, args ...[]interface{}) error {
	//glog.Infof("godbi SQL statement: %s", query)
	//glog.Infof("godbi input data: %v", args)

	n := len(args)
	if n == 0 {
		return self.DoSQL(query)
	} else if n == 1 {
		return self.DoSQL(query, args[0]...)
	}

	m := len(args[0])
	item := "(" + strings.Join(strings.Split(strings.Repeat("?", m), ""), ",") + ")"
	query += ""
	newArgs := make([]interface{}, 0)
	for _, item := range args[0] {
		newArgs = append(newArgs, item)
	}
	for i := 0; i < (n - 1); i++ {
		query += " " + item
		newArgs = append(newArgs, args[i+1]...)
	}
	return self.DoSQL(query, newArgs...)
}

// SelectSQL selects data rows as slice of maps into 'lists'.
// The data types in the rows are determined dynamically by the generic handle.
//
func (self *DBI) SelectSQL(lists *[]map[string]interface{}, query string, args ...interface{}) error {
	return self.SelectSQLTypeLabel(lists, nil, nil, query, args...)
}

// SelectSQLType selects data rows as slice of maps into 'lists'.
// The data types in the rows are predefined in the 'typeLabels'.
//
func (self *DBI) SelectSQLType(lists *[]map[string]interface{}, typeLabels []string, query string, args ...interface{}) error {
	return self.SelectSQLTypeLabel(lists, typeLabels, nil, query, args...)
}

// SelectSQLLabel selects data rows as slice of maps into 'lists'.
// The data types of the rows are determined dynamically by the generic handle.
// The original SQL column names will be renamed by 'selectLabels'.
//
func (self *DBI) SelectSQLLabel(lists *[]map[string]interface{}, selectLabels []string, query string, args ...interface{}) error {
	return self.SelectSQLTypeLabel(lists, nil, selectLabels, query, args...)
}

// SelectSQLTypeLabel selects data rows as slice of maps into 'lists'.
// The data types of the rows are predefined in the 'typeLabels'.
// The original SQL column names will be renamed by 'selectLabels'.
//
func (self *DBI) SelectSQLTypeLabel(lists *[]map[string]interface{}, typeLabels []string, selectLabels []string, query string, args ...interface{}) error {
	//glog.Infof("godbi SQL statement: %s", query)
	//glog.Infof("godbi select columns: %v", selectLabels)
	//glog.Infof("godbi column types: %v", typeLabels)
	//glog.Infof("godbi input data: %v", args)

	sth, err := self.DB.Prepare(query)
	if err != nil {
		return err
	}
	defer sth.Close()
	rows, err := sth.Query(Quotes(args)...)
	if err != nil {
		return err
	}
	defer rows.Close()

	return self.pickup(rows, lists, typeLabels, selectLabels, query)
}

func (self *DBI) pickup(rows *sql.Rows, lists *[]map[string]interface{}, typeLabels []string, selectLabels []string, query string) error {
	var err error
	if selectLabels == nil {
		if selectLabels, err = rows.Columns(); err != nil {
			return err
		}
	}

	isType := false
	if typeLabels != nil {
		isType = true
	}
	names := make([]interface{}, len(selectLabels))
	x := make([]interface{}, len(selectLabels))
	for i := range selectLabels {
		if isType {
			switch typeLabels[i] {
			case "int", "int8", "int16", "int32", "uint", "uint8", "uint16", "uint32", "int64":
				x[i] = new(sql.NullInt64)
			case "float32", "float64":
				x[i] = new(sql.NullFloat64)
			case "bool":
				x[i] = new(sql.NullBool)
			case "string":
				x[i] = new(sql.NullString)
			default:
			}
		} else {
			x[i] = &names[i]
		}
	}

	for rows.Next() {
		if err = rows.Scan(x...); err != nil {
			return err
		}
		res := make(map[string]interface{})
		for j, v := range selectLabels {
			if isType {
				switch typeLabels[j] {
				case "int":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = int(x.Int64)
					}
				case "int8":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = int8(x.Int64)
					}
				case "int16":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = int16(x.Int64)
					}
				case "int32":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = int32(x.Int64)
					}
				case "uint":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = uint(x.Int64)
					}
				case "uint8":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = uint8(x.Int64)
					}
				case "uint16":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = uint16(x.Int64)
					}
				case "uint32":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = uint32(x.Int64)
					}
				case "int64":
					x := x[j].(*sql.NullInt64)
					if x.Valid {
						res[v] = x.Int64
					}
				case "float32":
					x := x[j].(*sql.NullFloat64)
					if x.Valid {
						res[v] = float32(x.Float64)
					}
				case "float64":
					x := x[j].(*sql.NullFloat64)
					if x.Valid {
						res[v] = x.Float64
					}
				case "bool":
					x := x[j].(*sql.NullBool)
					if x.Valid {
						res[v] = x.Bool
					}
				case "string":
					x := x[j].(*sql.NullString)
					n := len(x.String)
					if x.Valid {
						if n >= 2 {
							res[v] = strings.TrimRight(x.String[:n-2], "\x00")
						} else {
							return errors.New("wrong string output: " + x.String)
						}
					}
				default:
				}
			} else {
				name := names[j]
				if name != nil {
					switch name.(type) {
					case []uint8:
						x := string(name.([]uint8))
						n := len(x)
						if n >= 2 {
							res[v] = strings.TrimRight(x[:n-2], "\x00")
						} else {
							return errors.New("wrong string output: " + x)
						}
					case string:
						x := name.(string)
						n := len(x)
						if n >= 2 {
							res[v] = strings.TrimRight(x[:n-2], "\x00")
						} else {
							return errors.New("wrong string output: " + x)
						}
					default:
						res[v] = name
					}
				}
			}
		}
		*lists = append(*lists, res)
	}
	if err := rows.Err(); err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}

// GetSQLLabel returns one row as map into 'res'.
// The column names are replaced by 'selectLabels'
//
func (self *DBI) GetSQLLabel(res map[string]interface{}, query string, selectLabels []string, args ...interface{}) error {
	lists := make([]map[string]interface{}, 0)
	if err := self.SelectSQLLabel(&lists, selectLabels, query, args...); err != nil {
		return err
	}
	if len(lists) >= 1 {
		for k, v := range lists[0] {
			if v != nil {
				res[k] = v
			}
		}
	}
	return nil
}

// GetArgs returns one row as url.Values into 'res', as in web application.
//
func (self *DBI) GetArgs(res url.Values, query string, args ...interface{}) error {
	lists := make([]map[string]interface{}, 0)
	if err := self.SelectSQL(&lists, query, args...); err != nil {
		return err
	}
	if len(lists) >= 1 {
		for k, v := range lists[0] {
			if v != nil {
				res.Set(k, interface2String(v))
			}
		}
	}
	return nil
}
