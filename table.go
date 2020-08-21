package taodbi

import (
	"encoding/json"
	"strings"
)

// Page type describes next page's structure
// Model: the name of the model
// Action: the method name on the model
// Manual: constraint conditions manually assigned
// RelateItem: current page's column versus next page's column. The value is forced as constraint.
type Page struct {
	Model      string                 `json:"model"`
	Action     string                 `json:"action"`
	Manual     map[string]interface{} `json:"manual,omitempty"`
	RelateItem map[string]string      `json:"relate_item,omitempty"`
}

func (self *Page) refresh(item map[string]interface{}, extra map[string]interface{}) (map[string]interface{}, bool) {
	newExtra := extra
	found := false
	for k, v := range self.RelateItem {
		if t, ok := item[k]; ok {
			found = true
			newExtra[v] = t
			break
		}
	}
	return newExtra, found
}

// Table gives the RESTful table structure, usually parsed by JSON file on disk
//
type Table struct {
	// CurrentTable: the current table name
	CurrentTable  string    `json:"current_table,omitempty"`
	// CurrentKey: the single primary key of the table
	CurrentKey    string    `json:"current_key,omitempty"`
	ForeignKey    string    `json:"foreign_key,omitempty"`
	Tags          []string  `json:"tags,omitempty"`
	// CurrentIDAuto: if the table has an auto assigned series number
	CurrentIDAuto string    `json:"current_id_auto,omitempty"`

	// Table columns for Crud
	// InsertPars: the columns used for Create
	InsertPars []string `json:"insert_pars,omitempty"`
	// EditPar: the columns used for Read One
	EditPars []interface{} `json:"edit_pars,omitempty"`
	// EditHash: the columns used for Read One
	EditHash map[string]interface{} `json:"edit_hash,omitempty"`
	// TopicsPars: the columns used for Read All
	TopicsPars []interface{} `json:"topics_pars,omitempty"`
	// TopicsHash: a map between SQL columns and output keys
	TopicsHash map[string]interface{} `json:"topics_hash,omitempty"`

	editHashPars interface{}
	topicsHashPars interface{}

	// TotalForce controls how the total number of rows be calculated for Topics
	// <-1	use ABS(TotalForce) as the total count
	// -1	always calculate the total count
	// 0	don't calculate the total count
	// 0	calculate only if the total count is not passed in args
	TotalForce int `json:"total_force,omitempty"`

	// Nextpages: defining how to call other models' actions
	Nextpages map[string][]*Page `json:"nextpages,omitempty"`

	Empties     string `json:"empties,omitempty"`
	Fields      string `json:"fields,omitempty"`
	//Maxpageno   string `json:"maxpageno,omitempty"`
	//Totalno     string `json:"totalno,omitempty"`
	Rowcount    string `json:"rawcount,omitempty"`
	//Pageno      string `json:"pageno,omitempty"`
	Sortreverse string `json:"sortreverse,omitempty"`
	//Sortby      string `json:"sortby,omitempty"`
	Passid      string `json:"passid,omitempty"`
}

func newTable(content []byte) (*Table, error) {
	var parsed *Table
	if err := json.Unmarshal(content, &parsed); err != nil {
		return nil, err
	}
	parsed.fulfill()
	return parsed, nil
}

func generalHashPars(TopicsHash map[string]interface{}, TopicsPars []interface{}, fields []string) interface{} {
	if hasValue(TopicsHash) {
		s2 := make(map[string][2]string)
		s1 := make(map[string]string)
		for k, vs := range TopicsHash {
			if fields != nil && len(fields)>0 && !grep(fields, k) {
				continue
			}
			switch v := vs.(type) {
			case []interface{}:
				s2[k] = [2]string{v[0].(string), v[1].(string)}
			default:
				s1[k] = v.(string)
			}
		}
		if len(s2) > 0 {
			return s2
		} else {
			return s1
		}
	} else {
		s2 := make([][2]string,0)
		s1 := make([]string,0)
		for _, vs := range TopicsPars {
			switch v := vs.(type) {
			case []interface{}:
				if fields != nil && len(fields)>0 && !grep(fields, v[0].(string)) {
					continue
				}
				s2 = append(s2, [2]string{v[0].(string), v[1].(string)})
			default:
				if fields != nil && len(fields)>0 && !grep(fields, v.(string)) {
					continue
				}
				s1 = append(s1, v.(string))
			}
		}
		if len(s2) > 0 {
			return s2
		} else {
			return s1
		}
	}
	return nil
}

func (parsed *Table) fulfill() {
	parsed.topicsHashPars = generalHashPars(parsed.TopicsHash, parsed.TopicsPars, nil)
	parsed.topicsHashPars = generalHashPars(parsed.TopicsHash, parsed.TopicsPars, nil)
	parsed.editHashPars   = generalHashPars(parsed.EditHash, parsed.EditPars, nil)

	if parsed.Sortreverse == "" {
		parsed.Sortreverse = "sortreverse"
	}
	if parsed.Rowcount == "" {
		parsed.Rowcount = "rowcount"
	}
	if parsed.Passid == "" {
		parsed.Passid = "passid"
	}
	if parsed.Fields == "" {
		parsed.Fields = "fields"
	}
	if parsed.Empties == "" {
		parsed.Empties = "empties"
	}
}

func (self *Table) statusColumn() string {
    for _, column := range self.InsertPars {
        if column != self.ForeignKey {
            return column
        }
    }
    return ""
}

// selectType returns variables' SELECT sql string, labels and types. 4 cases of interface{}
// []string{name}	just a list of column names
// [][2]string{name, type}	a list of column names and associated data types
// map[string]string{name: label}	rename the column names by labels
// map[string][2]string{name: label, type}	rename the column names to labels and use the specific types
//
func selectType(selectPars interface{}, isLast ...bool) (string, []string, []string) {
	if selectPars == nil {
		return "", nil, nil
	}

	switch vs := selectPars.(type) {
	case []string:
		labels := make([]string, 0)
		for _, v := range vs {
			labels = append(labels, v)
		}
		if hasValue(isLast) && isLast[0] {
			return "LAST(" + strings.Join(labels, "), LAST(") + ")", labels, nil
		}
		return strings.Join(labels, ", "), labels, nil
	case [][2]string:
		labels := make([]string, 0)
		types := make([]string, 0)
		for _, v := range vs {
			labels = append(labels, v[0])
			types = append(labels, v[1])
		}
		if hasValue(isLast) && isLast[0] {
			return "LAST(" + strings.Join(labels, "), LAST(") + ")", labels, types
		}
		return strings.Join(labels, ", "), labels, types
	case map[string]string:
		labels := make([]string, 0)
		keys := make([]string, 0)
		for key, val := range vs {
			keys = append(keys, key)
			labels = append(labels, val)
		}
		if hasValue(isLast) && isLast[0] {
			return "LAST(" + strings.Join(keys, "), LAST(") + ")", labels, nil
		}
		return strings.Join(keys, ", "), labels, nil
	case map[string][2]string:
		labels := make([]string, 0)
		keys := make([]string, 0)
		types := make([]string, 0)
		for key, val := range vs {
			keys = append(keys, key)
			labels = append(labels, val[0])
			types = append(labels, val[1])
		}
		if hasValue(isLast) && isLast[0] {
			return "LAST(" + strings.Join(keys, "), LAST(") + ")", labels, types
		}
		return strings.Join(keys, ", "), labels, types
	default:
	}

	if hasValue(isLast) && isLast[0] {
		return "LAST(" + selectPars.(string) + ")", []string{selectPars.(string)}, nil
	}
	return selectPars.(string), []string{selectPars.(string)}, nil
}

// selectCondition returns the WHERE constraint
// 1) if key has single value, it means a simple EQUAL constraint
// 2) if key has array values, it mean an IN constrain
// 3) if key is "_gsql", it means a raw SQL statement.
// 4) it is the AND condition between keys.
//
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

		switch vs := value.(type) {
		case []interface{}:
			n := len(vs)
			sql += field + " IN (" + strings.Join(strings.Split(strings.Repeat("?", n), ""), ",") + ")"
			for _, v := range vs {
				values = append(values, v)
			}
		case int, int64, int32, uint32, int16, uint16, int8, uint8:
			sql += field + "=?"
			values = append(values, vs)
		case string:
			if len(field) >= 5 && field[(len(field)-5):len(field)] == "_gsql" {
				sql += vs
			} else {
				sql += field + "=?"
				values = append(values, vs)
			}
		default:
		}
		sql += ")"
	}

	return sql, values
}

// singleCondition returns WHERE constrains in existence of ids.
// ids should be a slice of targeted values of keyname
// E.g. to select a single PK equaling to 1234, just use ids = []int{1234}
//
func singleCondition(keyname string, ids []interface{}, extra ...map[string]interface{}) (string, []interface{}) {
	sql := ""
	extraValues := make([]interface{}, 0)

	n := len(ids)
	if n > 1 {
		sql = "(" + keyname + " IN (" + strings.Join(strings.Split(strings.Repeat("?", n), ""), ",") + "))"
	} else {
		sql = "(" + keyname + "=?)"
	}
	for _, v := range ids {
		extraValues = append(extraValues, v)
	}


	if hasValue(extra) {
		s, arr := selectCondition(extra[0])
		sql += " AND " + s
		for _, v := range arr {
			extraValues = append(extraValues, v)
		}
	}

	return sql, extraValues
}
