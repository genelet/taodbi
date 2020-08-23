package taodbi

import (
	"testing"
	"time"
	"database/sql"
	_ "github.com/taosdata/driver-go/taosSql"
)

func TestCrudFilterExtra(t *testing.T) {
	extra := map[string]interface{}{"x":[]int{1,2,3,4}, "y":"a", "z":"c"}
	extraNew := filterExtra([]string{"x","z"}, extra)
	if len(extraNew["x"].([]int))!=4 || extraNew["z"].(string) != "c" {
		t.Errorf("%#v", extraNew["x"].([]int))
		t.Errorf("%#v", extraNew["z"].(string))
	}
}

func TestCrudDb(t *testing.T) {
	c := newconf("config.json")
	db, err := sql.Open(c.Db_type, c.Dsn_2)
	if err != nil {
		panic(err)
	}
	dbi := DBI{DB: db}
	crud := &crud{DBI: dbi, Table:Table{CurrentTable: "atesting", CurrentKey: "id"}}

	err = crud.DoSQL(`create database if not exists demodb precision "us"`)
	if err != nil {
		panic(err)
	}
	err = crud.DoSQL(`drop table if exists atesting`)
	if err != nil {
		panic(err)
	}
	err = crud.DoSQL(`drop table if exists testing`)
	if err != nil {
		panic(err)
	}
	err = crud.DoSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8))`)
	if err != nil {
		panic(err)
	}
	id := time.Now().UnixNano() / int64(time.Microsecond)
	hash := map[string]interface{}{"id": id, "x": "a1234567", "y": "b1234567"}
	err = crud.insertHash(hash)
	if err != nil {
		panic(err)
	}
	if crud.Affected != 1 {
		t.Errorf("%d wanted", crud.Affected)
	}
	hash = map[string]interface{}{"x": "c1234567", "y": "d1234567"}
	err = crud.insertHash(hash)
	if err != nil {
		panic(err)
	}

	lists := make([]map[string]interface{}, 0)
	edit_pars := []string{"id", "x", "y"}
	err = crud.editHash(&lists, edit_pars, []interface{}{id})
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
	err = crud.topicsHash(&lists, select_pars, "")
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
	err = crud.totalHash(&what)
	if err != nil {
		panic(err)
	}
	if what != 2 {
		t.Errorf("2 expected but %d found", what)
	}

	lists = make([]map[string]interface{}, 0)
	select_pars = []string{"id", "x", "y"}
	extra := map[string]interface{}{"x": "a1234567"}
	err = crud.topicsHash(&lists, select_pars, "", extra)
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

func TestCrudEditFK(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    dbi := DBI{DB: db}
    crud := &crud{DBI: dbi, Table:Table{CurrentTable: "tmain", CurrentKey: "id", ForeignKey: "x", InsertPars: []string{"x","y"}}}
    err = crud.DoSQL(`create database if not exists demodb precision "us"`)
    if err != nil {
        panic(err)
    }
    crud.DoSQL(`drop table if exists tmain`)
    crud.DoSQL(`create table tmain (id timestamp, x binary(8), y binary(8))`)
    id := time.Now().UnixNano() / int64(time.Microsecond)
    hash := map[string]interface{}{"id": id, "x": "a1234567", "y": "b1234567"}
    if err = crud.insertHash(hash); err != nil { panic(err) }
    id1 := time.Now().UnixNano() / int64(time.Microsecond)
    hash = map[string]interface{}{"id": id1, "x": "c1234567", "y": "d1234567"}
    if err = crud.insertHash(hash); err != nil { panic(err) }
    id2 := time.Now().UnixNano() / int64(time.Microsecond)
    hash = map[string]interface{}{"id": id2, "x": "a1234567", "y": "f1234567"}
    if err = crud.insertHash(hash); err != nil { panic(err) }
    id3 := time.Now().UnixNano() / int64(time.Microsecond)
    hash = map[string]interface{}{"id": id3, "x": "g1234567", "y": "h1234567"}
    if err = crud.insertHash(hash); err != nil { panic(err) }

    ids := []interface{}{"a1234567","c1234567","g1234567"}
	lists := make([]map[string]interface{}, 0)
    err = crud.editHashFK(&lists, []string{"id","x","y"}, ids)
	if err != nil { panic(err) }
	if len(lists) != 3 ||
		lists[0]["x"] != "a1234567" || lists[0]["y"] != "f1234567" ||
		lists[1]["x"] != "c1234567" || lists[1]["y"] != "d1234567" ||
		lists[2]["x"] != "g1234567" || lists[2]["y"] != "h1234567" {
		t.Errorf("%#v", lists)
	}

    extra := map[string]interface{}{"y":"h1234567"}
	lists = make([]map[string]interface{}, 0)
    err = crud.editHashFK(&lists, []string{"id","x","y"}, ids, extra)
	if err != nil { panic(err) }
	if len(lists) != 1 ||
		lists[0]["x"] != "g1234567" || lists[0]["y"] != "h1234567" {
		t.Errorf("%#v", lists)
	}

//    crud.DoSQL(`drop table if exists tmain`)
}
