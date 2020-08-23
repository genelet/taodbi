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
	model, err := NewModel("m1.json")
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
	model, err := NewModel("m1.json")
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
    model, err := NewModel("m1.json")
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
