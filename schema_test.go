package taodbi

import (
	"database/sql"
	"testing"
)

func TestSchemaModel(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
    model, err := NewModel("m22.json")
    if err != nil { panic(err) }
    model.SetDB(db)

	err = model.DoSQL(`drop table if exists atesting`)
	if err != nil { panic(err) }
	err = model.DoSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8), z binary(8))`)
	if err != nil { panic(err) }

    hash := map[string]interface{}{"x":"a1234567","y":"b1234567"}
    model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }
    id1 := model.LastID

    hash = map[string]interface{}{"x":"c1234567","y":"d1234567","z":"e1234"}
    model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }
    id2 := model.LastID

    hash = map[string]interface{}{"x":"e1234567","y":"f1234567","z":"e1234"}
    model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }
    id3 := model.LastID

	supp, err := NewModel("m33.json")
    if err != nil { panic(err) }
    supp.SetDB(db)

    err = supp.DoSQL(`drop table if exists testing`)
    if err != nil { panic(err) }
    err = supp.DoSQL(`CREATE TABLE testing (tid timestamp, child binary(8), id bigint)`)
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id1,"child":"john"}
    supp.SetArgs(hash)
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id1,"child":"sam"}
    supp.SetArgs(hash)
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id2,"child":"mary"}
    supp.SetArgs(hash)
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id3,"child":"kkk"}
    supp.SetArgs(hash)
    err = supp.Insert()
    if err != nil { panic(err) }

	st, err := NewModel("m3.json")
    if err != nil { panic(err) }
    st.SetDB(db)

	ss := make(map[string]func(...map[string]interface{}) error)
    ss["topics"] = func(args ...map[string]interface{}) error { return model.Topics(args...) }
    model.Actions = ss
	tt := make(map[string]func(...map[string]interface{}) error)
    tt["topics"] = func(args ...map[string]interface{}) error { return st.Topics(args...) }
	st.Actions = tt

    storage := NewSchema(map[string]Navigate{"s": model, "testing": st})
    storage.SetDB(db)

	lists, err := storage.Run("s", "topics", map[string]interface{}{})
    if err != nil { panic(err) }
// []map[string]interface {}{map[string]interface {}{"id":1576360769063, "x":"a1234567", "y":"b1234567"}, map[string]interface {}{"id":1576360769065, "x":"c1234567", "y":"d1234567", "z":"e1234"}, map[string]interface {}{"id":1576360769067, "x":"e1234567", "y":"f1234567", "z":"e1234"}}
// map[string]interface {}{"testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"john", "id":1576360769063, "tid":1576360769068}, map[string]interface {}{"child":"sam", "id":1576360769063, "tid":1576360769070}, map[string]interface {}{"child":"mary", "id":1576360769065, "tid":1576360769072}, map[string]interface {}{"child":"kkk", "id":1576360769067, "tid":1576360769073}}}

    list0 := lists[0]
    if len(lists) != 3 ||
        list0["x"].(string) != "a1234567" {
        t.Errorf("%#v", list0)
    }

	db.Close()
}

func TestSchema(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { t.Fatal(err) }
    defer db.Close()

	model, err := NewRmodel("m2.json")
	if err != nil {
		panic(err)
	}
	model.DB = db

	model.DoSQL(`drop table if exists at_status`)
	model.DoSQL(`drop table if exists at_profile`)
	model.DoSQL(`drop table if exists atesting`)
	model.DoSQL(`create table atesting ( id timestamp, x binary(8), y binary(8))`)
	model.DoSQL(`create table at_profile ( ts timestamp, id bigint, x binary(8), y binary(8), z binary(8))`)
	model.DoSQL(`create table if not exists at_status ( ts timestamp, id bigint, status bool)`)

	model.DoSQL(`drop table if exists t_status`)
	model.DoSQL(`drop table if exists t_profile`)
	model.DoSQL(`drop table if exists testing`)
	model.DoSQL(`create table testing ( tid timestamp, useless bool)`)
	model.DoSQL(`create table t_profile ( ts timestamp, tid bigint, id bigint, child binary(8))`)
	model.DoSQL(`create table if not exists t_status ( ts timestamp, tid bigint, status bool)`)


	hash := map[string]interface{}{"x": "a1234567", "y": "b1234567"}
	model.SetDB(db)
	model.SetArgs(hash)
	err = model.Insupd()
	if err != nil {
		panic(err)
	}
	id1 := model.LastID

	hash = map[string]interface{}{"x": "c1234567", "y": "d1234567", "z": "e1234"}
	model.SetArgs(hash)
	err = model.Insupd()
	if err != nil {
		panic(err)
	}
	id2 := model.LastID

	hash = map[string]interface{}{"x": "e1234567", "y": "f1234567", "z": "e1234"}
	model.SetArgs(hash)
	err = model.Insupd()
	if err != nil {
		panic(err)
	}
	id3 := model.LastID

	supp, err := NewRmodel("m3.json")
	if err != nil {
		panic(err)
	}
	supp.SetDB(db)

	hash = map[string]interface{}{"id": id1, "child": "john"}
	supp.SetArgs(hash)
	err = supp.Insert()
	if err != nil {
		panic(err)
	}

	hash = map[string]interface{}{"id": id1, "child": "sam"}
	supp.SetArgs(hash)
	err = supp.Insert()
	if err != nil {
		panic(err)
	}

	hash = map[string]interface{}{"id": id2, "child": "mary"}
	supp.SetArgs(hash)
	err = supp.Insert()
	if err != nil {
		panic(err)
	}

	hash = map[string]interface{}{"id": id3, "child": "kkk"}
	supp.SetArgs(hash)
	err = supp.Insert()
	if err != nil {
		panic(err)
	}

	st, err := NewRmodel("m3.json")
	if err != nil {
		panic(err)
	}

	ss := make(map[string]func(...map[string]interface{}) error)
	ss["topics"] = func(args ...map[string]interface{}) error { return model.Topics(args...) }
	model.Actions = ss
	tt := make(map[string]func(...map[string]interface{}) error)
	tt["topics"] = func(args ...map[string]interface{}) error { return st.Topics(args...) }
	st.Actions = tt

	schema := NewSchema(map[string]Navigate{"s": model, "testing": st})
	schema.SetDB(db)

	lists, err := schema.Run("s", "topics", map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	// []map[string]interface {}{map[string]interface {}{"id":1597983956779095, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"john", "id":1597983956779095, "tid":1597983956784481}, map[string]interface {}{"child":"sam", "id":1597983956779095, "tid":1597983956785550}}, "x":"a1234567", "y":"b1234567"}, map[string]interface {}{"id":1597983956781479, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"mary", "id":1597983956781479, "tid":1597983956786348}}, "x":"c1234567", "y":"d1234567", "z":"e1234"}, map[string]interface {}{"id":1597983956783365, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"kkk", "id":1597983956783365, "tid":1597983956787141}}, "x":"e1234567", "y":"f1234567", "z":"e1234"}}
	list0 := lists[0]
	relate := list0["testing_topics"].([]map[string]interface{})
	if len(lists) != 3 ||
		list0["x"].(string) != "a1234567" ||
		len(relate) != 2 ||
		relate[0]["child"].(string) != "john" {
		t.Errorf("%#v", list0)
		t.Errorf("%#v", relate)
	}

	db.Close()
}
