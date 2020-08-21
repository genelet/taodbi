package taodbi

import (
	"database/sql"
	"testing"
)

func TestRmodel(t *testing.T) {
	c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { t.Fatal(err) }
    defer db.Close()

    model, err := NewRmodel("rest.json")
    if err != nil { t.Fatal(err) }
	model.SetDB(db)

    if model.Crud.Table.CurrentTable != "tmain" {
        t.Errorf("%v", model.Crud)
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
