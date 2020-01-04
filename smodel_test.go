package taodbi

import (
    "testing"
	"time"
//	"strconv"
//	"math/rand"
    "database/sql"
    _ "taosSql"
)

func TestSmodel(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }

	smodel, err := NewSmodel(getString("ms.json"))
    if err != nil { panic(err) }
	smodel.Db = db
	err = smodel.ExecSQL(`drop table if exists stesting_333_yyz`)
	err = smodel.ExecSQL(`drop table if exists stesting`)
	if err != nil { panic(err) }
	err = smodel.ExecSQL(`CREATE TABLE stesting (id timestamp, x binary(8), y binary(8), z binary(8)) TAGS (pubid int, location binary(8))`)
	if err != nil { panic(err) }

	args := map[string]interface{}{"x":"aa1", "y":"bb1", "z":"cc1", "pubid":333, "location":"yyz"}
	smodel.ARGS = args
	err = smodel.Insert()
	if err != nil { panic(err) }
	time.Sleep(1 * time.Millisecond)

	args  = map[string]interface{}{"x":"aa2", "y":"bb2", "z":"cc2", "pubid":333, "location":"yyz"}
	smodel.ARGS = args
	err = smodel.Insert()
	if err != nil { panic(err) }
	time.Sleep(1 * time.Millisecond)

	args  = map[string]interface{}{"x":"aa3", "y":"bb3", "z":"cc3", "pubid":333, "location":"yyz"}
	smodel.ARGS = args
	err = smodel.Insert()
	if err != nil { panic(err) }

	smodel.LISTS = make([]map[string]interface{},0)
	err = smodel.Topics()
	if err != nil { panic(err) }
	lists := smodel.LISTS
	if len(lists) != 3 ||
		lists[0]["x"] != "aa1" || lists[0]["y"] != "bb1" ||
		lists[1]["x"] != "aa2" || lists[1]["y"] != "bb2" ||
		lists[2]["x"] != "aa3" || lists[2]["y"] != "bb3" {
		t.Errorf("%v", smodel.LISTS)
	}

	smodel.LISTS = make([]map[string]interface{},0)
	smodel.ARGS = map[string]interface{}{"x":"aa2"}
	err = smodel.LastTopics()
	lists = smodel.LISTS
	if err != nil { panic(err) }
	if len(lists) != 1 ||
		lists[0]["x"] != "aa2" || lists[0]["y"] != "bb2" {
		t.Errorf("%v", smodel.LISTS)
	}

	smodel.LISTS = make([]map[string]interface{},0)
	smodel.ARGS = map[string]interface{}{"x":"aa2", "pubid":333, "location":"yyz"}
	err = smodel.LastEdit()
	if err != nil { panic(err) }
	lists = smodel.LISTS
	if len(lists) != 1 ||
		lists[0]["x"] != "aa2" || lists[0]["y"] != "bb2" {
		t.Errorf("%v", smodel.LISTS)
	}

	db.Close()
}
