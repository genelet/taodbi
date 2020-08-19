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

	rest, err := newRestful(db, "rest.json")
	if err != nil { t.Fatal(err) }
	if rest.Crud.Table.CurrentTable != "tmain" {
		t.Errorf("%v", rest.Crud)
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
	t.Errorf("%v", lists)
}