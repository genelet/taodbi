package taodbi

import (
	"testing"
	"time"
	"database/sql"
	_ "github.com/taosdata/driver-go/taosSql"
)

func TestCrudStr(t *testing.T) {
	select_par := "firstname"
	sql, labels, types := selectType(select_par)
	if sql != "firstname" || labels[0] != "firstname" {
		t.Errorf("%s wanted", sql)
	}
	if types != nil {
		t.Errorf("nil wanted but %#v", types)
	}

	select_pars := []string{"firstname", "lastname", "id"}
	sql, labels, types = selectType(select_pars)
	if sql != "firstname, lastname, id" &&
		sql != "firstname, id, lastname" &&
		sql != "id, firstname, lastname" &&
		sql != "id, lastname, firstname" &&
		sql != "lastname, id, firstname" &&
		sql != "lastname, firstname, id" {
		t.Errorf("%s wanted", sql)
	}
	if types != nil {
		t.Errorf("nil wanted but %#v", types)
	}

	select_hash := map[string]string{"firstname": "string", "lastname": "string", "id": "int64"}
	sql, labels, types = selectType(select_hash)
	if sql != "id, firstname, lastname" &&
		sql != "id, lastnaem, firstname" &&
		sql != "firstname, lastname, id" &&
		sql != "firstname, id, lastname" &&
		sql != "lastname, firstname, id" &&
		sql != "lastname, id, firstname" {
		t.Errorf("%s wanted", sql)
	}

	extra := map[string]interface{}{"firstname": "Peter"}
	sql, c := selectCondition(extra)
	if sql != "(firstname =?)" {
		t.Errorf("%s wanted", sql)
	}
	if c[0].(string) != "Peter" {
		t.Errorf("%s wanted", c[0].(string))
	}

	extra = map[string]interface{}{"firstname": "Peter", "lastname": "Tong", "id": []int{1, 2, 3, 4}}
	sql, c = selectCondition(extra)
	if sql == "(firstname =?) AND (lastname =?) AND (id IN (?,?,?,?))" {
		if c[0].(string) != "Peter" {
			t.Errorf("%s wanted", c[0].(string))
		}
		if c[1].(string) != "Tong" {
			t.Errorf("%s wanted", c[1].(string))
		}
		if c[2].(int) != 1 {
			t.Errorf("%d wanted", c[2].(int))
		}
		if c[3].(int) != 2 {
			t.Errorf("%d wanted", c[3].(int))
		}
		if c[4].(int) != 3 {
			t.Errorf("%d wanted", c[4].(int))
		}
		if c[5].(int) != 4 {
			t.Errorf("%d wanted", c[5].(int))
		}
	}

	keyname := "user_id"
	ids := []interface{}{11, 22, 33, 44, 55}
	s, arr := singleCondition(keyname, ids, extra)
	if s == "(user_id IN (?,?,?,?,?)) AND (firstname =?) AND (lastname =?) AND (id IN (?,?,?,?))" {
		if arr[0].(int) != 11 {
			t.Errorf("%d wanted", arr[0].(int))
		}
		if arr[1].(int) != 22 {
			t.Errorf("%d wanted", arr[1].(int))
		}
		if arr[2].(int) != 33 {
			t.Errorf("%d wanted", arr[2].(int))
		}
		if arr[3].(int) != 44 {
			t.Errorf("%d wanted", arr[3].(int))
		}
		if arr[4].(int) != 55 {
			t.Errorf("%d wanted", arr[4].(int))
		}
		if arr[5] != "Peter" {
			t.Errorf("%s wanted", arr[5])
		}
	}
}

func TestCrudDb(t *testing.T) {
	c := newconf("config.json")
	db, err := sql.Open(c.Db_type, c.Dsn_2)
	if err != nil {
		panic(err)
	}
	dbi := DBI{Db: db}
	crud := &Crud{DBI: dbi, CurrentTable: "atesting", CurrentKey: "id"}

	err = crud.ExecSQL(`create database if not exists demodb precision "us"`)
	if err != nil {
		panic(err)
	}
	err = crud.ExecSQL(`drop table if exists atesting`)
	if err != nil {
		panic(err)
	}
	err = crud.ExecSQL(`drop table if exists testing`)
	if err != nil {
		panic(err)
	}
	err = crud.ExecSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8))`)
	if err != nil {
		panic(err)
	}
	id := time.Now().UnixNano() / int64(time.Microsecond)
	hash := map[string]interface{}{"id": id, "x": "a1234567", "y": "b1234567"}
	err = crud.InsertHash(hash)
	if err != nil {
		panic(err)
	}
	if crud.Affected != 1 {
		t.Errorf("%d wanted", crud.Affected)
	}
	hash = map[string]interface{}{"x": "c1234567", "y": "d1234567"}
	err = crud.InsertHash(hash)
	if err != nil {
		panic(err)
	}

	lists := make([]map[string]interface{}, 0)
	edit_pars := []string{"id", "x", "y"}
	err = crud.EditHash(&lists, edit_pars, id)
	if err != nil {
		panic(err)
	}
	if len(lists) != 1 {
		t.Errorf("%d records returned from edit", len(lists))
	}
	if lists[0]["x"].(string) != "a1234567" {
		t.Errorf("%s a1234567 wanted", lists[0]["x"].(string))
	}
	if lists[0]["y"].(string) != "b1234567" {
		t.Errorf("%s b1234567 wanted", string(lists[0]["y"].(string)))
	}

	lists = make([]map[string]interface{}, 0)
	select_pars := []string{"id", "x", "y"}
	err = crud.TopicsHash(&lists, select_pars)
	if err != nil {
		panic(err)
	}
	if len(lists) != 2 {
		t.Errorf("%d records returned from select, should be 2", len(lists))
	}
	if lists[0]["id"].(int64) != id {
		t.Errorf("%s wanted", string(lists[0]["x"].(string)))
	}
	if lists[0]["x"].(string) != "a1234567" {
		t.Errorf("%s a1234567 wanted", string(lists[0]["x"].(string)))
	}
	if lists[0]["y"].(string) != "b1234567" {
		t.Errorf("%s b1234567 wanted", string(lists[0]["y"].(string)))
	}
	if lists[1]["x"].(string) != "c1234567" {
		t.Errorf("%s c1234567 wanted", string(lists[1]["x"].(string)))
	}
	if lists[1]["y"].(string) != "d1234567" {
		t.Errorf("%s d1234567 wanted", string(lists[1]["y"].(string)))
	}

	what := int64(0)
	err = crud.TotalHash(&what)
	if err != nil {
		panic(err)
	}
	if what != 2 {
		t.Errorf("2 expected but %d found", what)
	}

	lists = make([]map[string]interface{}, 0)
	select_pars = []string{"id", "x", "y"}
	extra := map[string]interface{}{"x": "a1234567"}
	err = crud.TopicsHash(&lists, select_pars, extra)
	if err != nil {
		panic(err)
	}
	if len(lists) != 1 {
		t.Errorf("%d records returned from select, should be 1", len(lists))
	}
	if lists[0]["id"].(int64) != id {
		t.Errorf("%s wanted", string(lists[0]["x"].(string)))
	}
	if lists[0]["x"].(string) != "a1234567" {
		t.Errorf("%s a1234567 wanted", string(lists[0]["x"].(string)))
	}
	if lists[0]["y"].(string) != "b1234567" {
		t.Errorf("%s b1234567 wanted", string(lists[0]["y"].(string)))
	}

	db.Close()
}
