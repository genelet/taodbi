package taodbi

import (
    "testing"
	"time"
	"strconv"
	"math/rand"
    "database/sql"
    _ "github.com/taosdata/driver-go/taosSql"
)

func TestModel(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
	model, err := NewSmodel("m1.json")
    if err != nil { panic(err) }
	model.SetDB(db)

	err = model.DoSQL(`drop table if exists atesting`)
	if err != nil { panic(err) }
	err = model.DoSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8), z binary(8))`)
	if err != nil { panic(err) }

	//id := time.Now().UnixNano() / int64(time.Millisecond)
	hash := map[string]interface{}{"x":"a1234567","y":"b1234567"}
	model.SetArgs(hash)
	err = model.Insert()
	if err != nil { panic(err) }

	if model.Affected != 1 {
		t.Errorf("%d wanted", model.Affected)
	}
	hash = map[string]interface{}{"x":"c1234567","y":"d1234567","z":"e1234"}
	model.SetArgs(hash)
	err = model.Insert()
	if err != nil { panic(err) }
	hash = map[string]interface{}{"x":"f1234567","y":"g1234567","z":"e1234"}
	model.SetArgs(hash)
	err = model.Insert()
	if err != nil { panic(err) }

	err = model.Topics()
	if err != nil { panic(err) }
    lists := model.GetLists()
	if len(lists) !=3 {
		t.Errorf("%d records returned from topics", len(lists))
	}
	if (lists[0]["x"].(string) != "a1234567") {
		t.Errorf("%s a1234567 wanted", lists[0]["x"].(string))
	}
	if (lists[0]["y"].(string) != "b1234567") {
		t.Errorf("%s b1234567 wanted", lists[0]["y"].(string))
	}
	if (lists[1]["x"].(string) != "c1234567") {
		t.Errorf("%s c1234567 wanted", lists[1]["x"].(string))
	}
	if (lists[1]["y"].(string) != "d1234567") {
		t.Errorf("%s d1234567 wanted", lists[1]["y"].(string))
	}
	if (lists[1]["z"].(string) != "e1234") {
		t.Errorf("%#v, %s e1234 wanted", lists[1], lists[1]["z"].(string))
	}

	err = model.Topics(map[string]interface{}{"z":"e1234"})
	if err != nil { panic(err) }
    lists = model.GetLists()
	if len(lists) !=2 {
		t.Errorf("%d records returned from topics", len(lists))
	}
	if (lists[0]["x"].(string) != "c1234567") {
		t.Errorf("%s c1234567 wanted", lists[0]["x"].(string))
	}
	if (lists[0]["y"].(string) != "d1234567") {
		t.Errorf("%s d1234567 wanted", lists[0]["y"].(string))
	}
	if (lists[1]["x"].(string) != "f1234567") {
		t.Errorf("%s f1234567 wanted", lists[1]["x"].(string))
	}
	if (lists[1]["y"].(string) != "g1234567") {
		t.Errorf("%s g1234567 wanted", lists[1]["y"].(string))
	}
	if (lists[1]["z"].(string) != "e1234") {
		t.Errorf("%#v, %s e1234 wanted", lists[1], lists[1]["z"].(string))
	}
id := lists[1]["id"].(int64)

t1 := time.Now()
for i:=0; i<10000; i++ {
	hash = map[string]interface{}{"id":id}
	model.SetArgs(hash)
	err = model.Edit()
	if err != nil { panic(err) }
    lists = model.GetLists()
	if len(lists) !=1 {
		t.Errorf("%d records returned from topics", len(lists))
	}
	if (lists[0]["x"].(string) != "f1234567") {
		t.Errorf("%s f1234567 wanted", lists[0]["x"].(string))
	}
	if (lists[0]["y"].(string) != "g1234567") {
		t.Errorf("%s g1234567 wanted", lists[0]["y"].(string))
	}
	if (lists[0]["z"].(string) != "e1234") {
		t.Errorf("%s e1234 wanted", lists[0]["z"].(string))
	}
}
t2 := time.Now()
diff := t2.Sub(t1).Seconds()
if diff > 10.0 {
	t.Errorf("sould take 2 seconds but you got: %6.6f", diff)
}

	db.Close()
}

func TestPagination(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
	model, err := NewSmodel("m1.json")
    if err != nil { panic(err) }
	model.SetDB(db)

	err = model.DoSQL(`drop table if exists atesting`)
	if err != nil { panic(err) }
	err = model.DoSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8), z binary(8))`)
	if err != nil { panic(err) }

	str := model.orderString()
	if str != "ORDER BY id" {
		t.Errorf("id expected, got %s", str)
	}
	model.SetArgs(map[string]interface{}{"sortreverse":1, "rowcount":20})
	str = model.orderString()
	if str != "ORDER BY id DESC LIMIT 20 OFFSET 0" {
		t.Errorf("'id DESC LIMIT 20 OFFSET 0' expected, got %s", str)
	}
	model.SetArgs(map[string]interface{}{"sortreverse":1, "rowcount":20, "pageno":5})
	str = model.orderString()
	if str != "ORDER BY id DESC LIMIT 20 OFFSET 80" {
		t.Errorf("'id DESC LIMIT 20 OFFSET 80' expected, got %s", str)
	}

	err = model.DoSQL(`drop table if exists atesting`)
	if err != nil { panic(err) }
	err = model.DoSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8), z binary(8))`)
	if err != nil { panic(err) }

	//id := time.Now().UnixNano() / int64(time.Millisecond)
	for i:=0; i<100; i++ {
		hash := map[string]interface{}{"x":"a1234567","y":"b1234567"}
		r := strconv.Itoa(int(rand.Int31()))
		if len(r)>8 { r=r[0:8] }
		hash["z"] = r
		model.SetArgs(hash)
		err = model.Insert()
		if err != nil { panic(err) }
	}
	model.SetArgs(map[string]interface{}{"rowcount":20})
	err = model.Topics()
	if err != nil { panic(err) }
    lists := model.GetLists()
	if len(lists) !=20 {
		t.Errorf("%d records returned from topics", len(lists))
	}

	model.SetArgs(map[string]interface{}{"sortreverse":1, "rowcount":20, "pageno":5})
	str = model.orderString()
	if str != "ORDER BY id DESC LIMIT 20 OFFSET 80" {
		t.Errorf("'ORDER BY id DESC LIMIT 20 OFFSET 80' expected, got %s", str)
	}
	err = model.Topics()
	if err != nil { panic(err) }
	args := model.getArgs()
	if args["totalno"].(int) != 100 {
		t.Errorf("100 records expected, but %#v", args)
	}
	db.Close()
}

func TestUInsupd(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
    model, err := NewSmodel("m1.json")
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
	id := model.LastID

    hash = map[string]interface{}{"x":"c1234567","y":"d1234567","z":"e1234"}
	model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"x":"a1234567","y":"b1234567","z":"e1234"}
	model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }
	if !model.Updated {
		t.Errorf("%#v", model.Updated)
	}
	if model.LastID != id {
		t.Errorf("%#v %#v", model.LastID, id)
	}

	model.SetArgs(make(map[string]interface{}))
	err = model.Topics()
    if err != nil { panic(err) }
	lists := model.GetLists()
	if len(lists) != 2 {
		t.Errorf("%#v", lists)
	}
	db.Close()
}

/*
func TestNextPages(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
    model, err := NewSmodel(getString("m2.json"))
    if err != nil { panic(err) }
    model.SetDB(db)
    model.SetArgs(make(map[string]interface{})

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
    id2 := model.CurrentRow["id"].(int64)

    hash = map[string]interface{}{"x":"e1234567","y":"f1234567","z":"e1234"}
    model.SetArgs(hash)
    err = model.Insupd()
    if err != nil { panic(err) }
    id3 := model.CurrentRow["id"].(int64)



	supp, err := NewSmodel(getString("m3.json"))
    if err != nil { panic(err) }
    supp.SetDB(db)
    supp.SetArgs(make(map[string]interface{}))

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

	st, err := NewSmodel(getString("m3.json"))
    if err != nil { panic(err) }
    st.Db = db
    st.ARGS  = make(map[string]interface{})
    st.OTHER = make(map[string]interface{})

	storage := make(map[string]map[string]interface{})
	storage["model"]= make(map[string]interface{})
	storage["model"]["testing"]= st
	storage["action"]= make(map[string]interface{})
	tt := make(map[string]interface{})
	tt["topics"] = func(args ...map[string]interface{}) error {
        return st.Topics(args...)
    }
	storage["action"]["testing"] = tt

	model.Storage = storage
t1 := time.Now()
for i:=0; i<10000; i++ {
	err = model.Topics()
    if err != nil { panic(err) }
    lists := model.GetLists()
// []map[string]interface {}{map[string]interface {}{"id":1576360379162, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"john", "id":1576360379162, "tid":1576360379168}, map[string]interface {}{"child":"sam", "id":1576360379162, "tid":1576360379170}}, "x":"a1234567", "y":"b1234567"}, map[string]interface {}{"id":1576360379164, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"mary", "id":1576360379164, "tid":1576360379172}}, "x":"c1234567", "y":"d1234567", "z":"e1234"}, map[string]interface {}{"id":1576360379167, "testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"kkk", "id":1576360379167, "tid":1576360379174}}, "x":"e1234567", "y":"f1234567", "z":"e1234"}}
	list0 := lists[0]
	relate := list0["testing_topics"].([]map[string]interface{})
    if len(lists) != 3 ||
		list0["x"].(string) != "a1234567" ||
		len(relate) != 2 ||
		relate[0]["child"].(string) != "john" {
		t.Errorf("%#v", list0)
		t.Errorf("%#v", relate)
	}
}
t2 := time.Now()
diff := t2.Sub(t1).Seconds()
if diff > 30.0 {
    t.Errorf("sould take 8 seconds but you got: %6.6f", diff)
}

	db.Close()
}

func TestNextPagesMore(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
    model, err := NewSmodel(getString("m22.json")) // no relate_item, to OTHER
    if err != nil { panic(err) }
    model.Db = db
    model.ARGS  = make(map[string]interface{})
    model.OTHER = make(map[string]interface{})

	err = model.ExecSQL(`drop table if exists atesting`)
	if err != nil { panic(err) }
	err = model.ExecSQL(`CREATE TABLE atesting (id timestamp, x binary(8), y binary(8), z binary(8))`)
	if err != nil { panic(err) }

    hash := map[string]interface{}{"x":"a1234567","y":"b1234567"}
    model.ARGS = hash
    err = model.Insupd()
    if err != nil { panic(err) }
    id1 := model.CurrentRow["id"].(int64)

    hash = map[string]interface{}{"x":"c1234567","y":"d1234567","z":"e1234"}
    model.ARGS = hash
    err = model.Insupd()
    if err != nil { panic(err) }
    id2 := model.CurrentRow["id"].(int64)

    hash = map[string]interface{}{"x":"e1234567","y":"f1234567","z":"e1234"}
    model.ARGS = hash
    err = model.Insupd()
    if err != nil { panic(err) }
    id3 := model.CurrentRow["id"].(int64)


	supp, err := NewSmodel(getString("m3.json"))
    if err != nil { panic(err) }
    supp.Db = db
    supp.ARGS  = make(map[string]interface{})
    supp.OTHER = make(map[string]interface{})

    err = supp.ExecSQL(`drop table if exists testing`)
    if err != nil { panic(err) }
    err = supp.ExecSQL(`CREATE TABLE testing (tid timestamp, child binary(8), id bigint)`)
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id1,"child":"john"}
    supp.ARGS = hash
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id1,"child":"sam"}
    supp.ARGS = hash
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id2,"child":"mary"}
    supp.ARGS = hash
    err = supp.Insert()
    if err != nil { panic(err) }

    hash = map[string]interface{}{"id":id3,"child":"kkk"}
    supp.ARGS = hash
    err = supp.Insert()
    if err != nil { panic(err) }


	st, err := NewSmodel(getString("m3.json"))
    if err != nil { panic(err) }
    st.Db = db
    st.ARGS  = make(map[string]interface{})
    st.OTHER = make(map[string]interface{})

	storage := make(map[string]map[string]interface{})
	storage["model"]= make(map[string]interface{})
	storage["model"]["testing"]= st
	storage["action"]= make(map[string]interface{})
	tt := make(map[string]interface{})
	tt["topics"] = func(args ...map[string]interface{}) error {
        return st.Topics(args...)
    }
	storage["action"]["testing"] = tt

	model.Storage = storage
	err = model.Topics()
    if err != nil { panic(err) }
// []map[string]interface {}{map[string]interface {}{"id":1576360769063, "x":"a1234567", "y":"b1234567"}, map[string]interface {}{"id":1576360769065, "x":"c1234567", "y":"d1234567", "z":"e1234"}, map[string]interface {}{"id":1576360769067, "x":"e1234567", "y":"f1234567", "z":"e1234"}}
// map[string]interface {}{"testing_topics":[]map[string]interface {}{map[string]interface {}{"child":"john", "id":1576360769063, "tid":1576360769068}, map[string]interface {}{"child":"sam", "id":1576360769063, "tid":1576360769070}, map[string]interface {}{"child":"mary", "id":1576360769065, "tid":1576360769072}, map[string]interface {}{"child":"kkk", "id":1576360769067, "tid":1576360769073}}}

    lists := model.GetLists()
    list0 := lists[0]
    if len(lists) != 3 ||
        list0["x"].(string) != "a1234567" {
        t.Errorf("%#v", list0)
    }
    other := model.OTHER
    relate := other["testing_topics"].([]map[string]interface{})
    if len(relate) != 4 ||
        relate[0]["child"].(string) != "john" {
        t.Errorf("%#v", relate)
	}

	db.Close()
}
*/

func TestSmodel(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }

	smodel, err := NewSmodel("ms.json")
    if err != nil { panic(err) }
	smodel.SetDB(db)
	err = smodel.DoSQL(`drop table if exists stesting_333_yyz`)
	err = smodel.DoSQL(`drop table if exists stesting`)
	if err != nil { panic(err) }
	err = smodel.DoSQL(`CREATE TABLE stesting (id timestamp, x binary(8), y binary(8), z binary(8)) TAGS (pubid int, location binary(8))`)
	if err != nil { panic(err) }

	args := map[string]interface{}{"x":"aa1", "y":"bb1", "z":"cc1", "pubid":333, "location":"yyz"}
	smodel.SetArgs(args)
	err = smodel.Insert()
	if err != nil { panic(err) }

	args  = map[string]interface{}{"x":"aa2", "y":"bb2", "z":"cc2", "pubid":333, "location":"yyz"}
	smodel.SetArgs(args)
	err = smodel.Insert()
	if err != nil { panic(err) }

	args  = map[string]interface{}{"x":"aa3", "y":"bb3", "z":"cc3", "pubid":333, "location":"yyz"}
	smodel.SetArgs(args)
	err = smodel.Insert()
	if err != nil { panic(err) }

	err = smodel.Topics()
	if err != nil { panic(err) }
	lists := smodel.GetLists()
	if len(lists) != 3 ||
		lists[0]["x"] != "aa1" || lists[0]["y"] != "bb1" ||
		lists[1]["x"] != "aa2" || lists[1]["y"] != "bb2" ||
		lists[2]["x"] != "aa3" || lists[2]["y"] != "bb3" {
		t.Errorf("%v", smodel.GetLists())
	}

	smodel.SetArgs(map[string]interface{}{"x":"aa2"})
	err = smodel.LastTopics()
	lists = smodel.GetLists()
	if err != nil { panic(err) }
	if len(lists) != 1 ||
		lists[0]["x"] != "aa2" || lists[0]["y"] != "bb2" {
		t.Errorf("%v", smodel.GetLists())
	}

	smodel.SetArgs(map[string]interface{}{"x":"aa2", "pubid":333, "location":"yyz"})
	err = smodel.LastEdit()
	if err != nil { panic(err) }
	lists = smodel.GetLists()
	if len(lists) != 1 ||
		lists[0]["x"] != "aa2" || lists[0]["y"] != "bb2" {
		t.Errorf("%v", smodel.GetLists())
	}

	db.Close()
}
