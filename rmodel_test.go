package taodbi

import (
	"testing"
	"database/sql"
	_ "github.com/taosdata/driver-go/taosSql"
)

func initRest(db *sql.DB) {
	db.Exec(`create database if not exists demodb precision "us"`)
	db.Exec(`use demodb`)
	db.Exec("drop table if exists tstatus")
	db.Exec("drop table if exists tprofile")
	db.Exec("drop table if exists tmain")
	db.Exec("create table if not exists tmain ( id timestamp, username binary(16))")
	db.Exec("create table if not exists tprofile ( ts timestamp, id bigint, username binary(16), passwd binary(32), firstname binary(16), lastname binary(16), gender bool, street binary(32), city binary(32), province tinyint, phone binary(16), email binary(32))")
	db.Exec("create table if not exists tstatus ( ts timestamp, id bigint, status bool)")
}

func TestRestfulNew(t *testing.T) {
	c := newconf("config.json")
	db, err := sql.Open(c.Db_type, c.Dsn_2)
	if err != nil { t.Fatal(err) }
	defer db.Close()

	rest, err := NewRmodel("rest.json")
	if err != nil { t.Fatal(err) }
	rest.SetDB(db)
	if rest.crud.Table.CurrentTable != "tmain" {
		t.Errorf("%v", rest.crud)
	}
	if rest.ProfileTable.CurrentTable != "tprofile" {
		t.Errorf("%#v", rest.ProfileTable)
	}
	if rest.StatusTable.CurrentTable != "tstatus" {
		t.Errorf("%#v", rest.StatusTable)
	}
	if rest.StatusTable.statusColumn() != "status" {
		t.Errorf("%#v", rest.StatusTable)
	}

	initRest(db)

	args := map[string]interface{}{"username":"u1", "passwd":"p1", "firstname":"f1", "lastname":"l1", "gender":true, "street":"s1", "city":"c1", "province":1, "phone":"p1", "email":"e1"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id1 := rest.LastID

	args = map[string]interface{}{"username":"u2", "passwd":"p2", "firstname":"f2", "lastname":"l2", "gender":true, "street":"s2", "city":"c2", "province":2, "phone":"p2", "email":"e2"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id2 := rest.LastID

	args = map[string]interface{}{"username":"u3", "passwd":"p3", "firstname":"f3", "lastname":"l3", "gender":false, "street":"s3", "city":"c3", "province":2, "phone":"p3", "email":"e3"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id3 := rest.LastID

	args = map[string]interface{}{"username":"u4", "passwd":"p4", "firstname":"f4", "lastname":"l4", "gender":false, "street":"s4", "city":"c4", "province":2, "phone":"p4", "email":"e4"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id4 := rest.LastID

	args = map[string]interface{}{"username":"u5", "passwd":"p5", "firstname":"f5", "lastname":"l5", "gender":true, "street":"s5", "city":"c5", "province":5, "phone":"p5", "email":"e5"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id5 := rest.LastID

	lists := make([]map[string]interface{}, 0)
	ids := []interface{}{id1,id2,id3,id4,id5}
	if err = rest.editRest(&lists, rest.ProfileTable.editHashPars, ids); err != nil { t.Fatal(err) }
	if len(lists)!=5 || lists[3]["city"].(string) != "c4" {
		t.Errorf("%v", lists)
	}

	args = map[string]interface{}{"firstname":"f44", "lastname":"l44", "gender":false, "street":"s44", "city":"c44", "province":24, "phone":"p44", "email":"e44"}
	if err = rest.updateRest(args, []interface{}{id4}, nil); err != nil { t.Fatal(err) }

	lists = make([]map[string]interface{}, 0)
	ids = []interface{}{id1,id2,id3,id4,id5}
	if err = rest.editRest(&lists, rest.ProfileTable.editHashPars, ids); err != nil { t.Fatal(err) }
	if len(lists)!=5 || lists[3]["city"].(string) != "c44" {
		t.Errorf("%v", lists)
	}

	if err = rest.deleteRest([]interface{}{id4}); err != nil { t.Fatal(err) }

	lists = make([]map[string]interface{}, 0)
	if err = rest.topicsRest(100, false, 0, &lists, rest.ProfileTable.topicsHashPars); err != nil { t.Fatal(err) }
	if len(lists)!=4 || lists[3]["firstname"].(string) != "f5" {
		t.Errorf("%v", lists)
	}

	args = map[string]interface{}{"username":"u4", "passwd":"p4", "firstname":"f4", "lastname":"l4", "gender":false, "street":"s4", "city":"c4", "province":2, "phone":"p4", "email":"e4"}
	if err =  rest.insertRest(args); err != nil { t.Fatal(err) }
	id44 := rest.LastID
	lists = make([]map[string]interface{}, 0)
	if err = rest.topicsRest(100, false, 0, &lists, rest.ProfileTable.topicsHashPars); err != nil { t.Fatal(err) }
	if len(lists)!=5 || id4!=id44 || lists[3]["firstname"].(string) != "f4" {
		t.Errorf("%v", lists[3])
	}

	lists = make([]map[string]interface{}, 0)
	if err = rest.topicsRest(100, false, 0, &lists, rest.ProfileTable.topicsHashPars, map[string]interface{}{"province":2}); err != nil { t.Fatal(err) }
	if len(lists)!=3 || lists[0]["firstname"].(string) != "f2" ||
	                    lists[1]["firstname"].(string) != "f3" ||
	                    lists[2]["firstname"].(string) != "f4" {
		t.Errorf("%v", lists)
	}

	var start, end, v, n int64
	if err := rest.totalRest(&start, &end, &v, &n); err != nil { t.Fatal(err) }
	if start != id1 || end != id5 || v!=5 || n!=5 {
		t.Errorf("%d %d %d %d", start, end, v, n)
	}
	if err := rest.totalRest(&start, &end, &v, &n); err != nil { t.Fatal(err) }
	if start != id1 || end != id5 || v!=5 || n!=5 {
		t.Errorf("%d %d %d %d", start, end, v, n)
	}

	if err = rest.deleteRest([]interface{}{id4}); err != nil { t.Fatal(err) }
	if err := rest.totalRest(&start, &end, &v, &n); err != nil { t.Fatal(err) }
	if start != id1 || end != id5 || v!=4 || n!=5 {
		t.Errorf("%d %d %d %d", start, end, v, n)
	}
}

func TestRmodel(t *testing.T) {
	c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { t.Fatal(err) }
    defer db.Close()

    model, err := NewRmodel("rest.json")
    if err != nil { t.Fatal(err) }
	model.SetDB(db)

    if model.crud.Table.CurrentTable != "tmain" {
        t.Errorf("%v", model.crud)
    }
    if model.ProfileTable.CurrentTable != "tprofile" {
        t.Errorf("%#v", model.ProfileTable)
    }
    if model.StatusTable.CurrentTable != "tstatus" {
        t.Errorf("%#v", model.StatusTable)
    }
    if model.StatusTable.statusColumn() != "status" {
        t.Errorf("%#v", model.StatusTable)
    }

    initRest(db)

	args := map[string]interface{}{"username":"u1", "passwd":"p1", "firstname":"f1", "lastname":"l1", "gender":true, "street":"s1", "city":"c1", "province":1, "phone":"p1", "email":"e1"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }

    args = map[string]interface{}{"username":"u2", "passwd":"p2", "firstname":"f2", "lastname":"l2", "gender":true, "street":"s2", "city":"c2", "province":2, "phone":"p2", "email":"e2"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }

    args = map[string]interface{}{"username":"u3", "passwd":"p3", "firstname":"f3", "lastname":"l3", "gender":false, "street":"s3", "city":"c3", "province":2, "phone":"p3", "email":"e3"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }
	back := model.getArgs()

    args = map[string]interface{}{"username":"u4", "passwd":"p4", "firstname":"f4", "lastname":"l4", "gender":false, "street":"s4", "city":"c4", "province":2, "phone":"p4", "email":"e4"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }
	back = model.getArgs()
    id4 := back[model.CurrentKey]

    args = map[string]interface{}{"username":"u5", "passwd":"p5", "firstname":"f5", "lastname":"l5", "gender":true, "street":"s5", "city":"c5", "province":5, "phone":"p5", "email":"e5"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }

	args = map[string]interface{}{"id":id4}
	model.SetArgs(args)
    if err = model.Edit(); err != nil { t.Fatal(err) }
    lists := model.GetLists()
    if len(lists)!=1 || lists[0]["city"].(string) != "c4" {
        t.Errorf("%v", lists)
    }

    args = map[string]interface{}{"id":id4, "firstname":"f44", "lastname":"l44", "gender":false, "street":"s44", "city":"c44", "province":24, "phone":"p44", "email":"e44"}
	model.SetArgs(args)
    if err = model.Update(); err != nil { t.Fatal(err) }

	args = map[string]interface{}{"id":id4}
	model.SetArgs(args)
    if err = model.Edit(); err != nil { t.Fatal(err) }
    lists = model.GetLists()
    if len(lists)!=1 || lists[0]["city"].(string) != "c44" {
        t.Errorf("%v", lists)
    }

	args = map[string]interface{}{"id":id4}
	model.SetArgs(args)
    if err = model.Delete(); err != nil { t.Fatal(err) }

	args = map[string]interface{}{}
	model.SetArgs(args)
    if err = model.Topics(); err != nil { t.Fatal(err) }
    lists = model.GetLists()
    if len(lists)!=4 || lists[3]["firstname"].(string) != "f5" {
        t.Errorf("%v", lists)
    }

    args = map[string]interface{}{"username":"u4", "passwd":"p4", "firstname":"f4", "lastname":"l4", "gender":false, "street":"s4", "city":"c4", "province":2, "phone":"p4", "email":"e4"}
	model.SetArgs(args)
    if err =  model.Insert(); err != nil { t.Fatal(err) }
	back = model.getArgs()
    id44 := back[model.CurrentKey]

	args = map[string]interface{}{}
	model.SetArgs(args)
    if err = model.Topics(); err != nil { t.Fatal(err) }
    lists = model.GetLists()
    if len(lists)!=5 || id4!=id44 || lists[3]["firstname"].(string) != "f4" {
        t.Errorf("%v", lists)
    }

	args = map[string]interface{}{}
	model.SetArgs(args)
    if err = model.Topics(map[string]interface{}{"province":2}); err != nil { t.Fatal(err) }
    lists = model.GetLists()
    if len(lists)!=3 || lists[0]["firstname"].(string) != "f2" ||
                        lists[1]["firstname"].(string) != "f3" ||
                        lists[2]["firstname"].(string) != "f4" {
        t.Errorf("%v", lists)
    }
}
