package taodbi

import (
	"testing"
	"fmt"
	"time"
	"database/sql"
	_ "github.com/taosdata/driver-go/taosSql"
)

func TestQuote(t *testing.T) {
	a := `string`
	b := `str'ing`
	c := `str;ing`
	d := `str\'ing`
	x := `'string'`
	y := 1
	z := 32.657
	if Quote(a) != `'string'` {
		t.Errorf("%#v actual: %#v", a, Quote(a))
	}
	if Quote(b) != `'str\'ing'` {
		t.Errorf("%#v actual: %#v", b, Quote(b))
	}
	if Quote(c) != `'str\;ing'` {
		t.Errorf("%#v actual: %#v", c, Quote(c))
	}
	if Quote(d) != `'str\\'ing'` {
		t.Errorf("%#v actual: %#v", d, Quote(d))
	}
	if Quote(x) != `'string'` {
		t.Errorf("%#v actual: %#v", x, Quote(x))
	}
	if Quote(y) != 1 {
		t.Errorf("%#v actual: %#v", y, Quote(y))
	}
	if Quote(z) != 32.657 {
		t.Errorf("%#v actual: %#v", z, Quote(z))
	}
}

func TestLong(t *testing.T) {
	c := newconf("config.json")

	dbname := "demodb"
	db, err := Open(c.Dsn_1)
	if err != nil {
		panic(err)
	}
	dbi := &DBI{Db: db}

	err = dbi.ExecSQL(`drop database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`create database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`use ` + dbname)
	if err != nil {
		panic(err)
	}
	tb := "demot"
	err = dbi.ExecSQL("create table " + tb + " (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)")
	if err != nil {
		panic(err)
	}

	st := time.Now()
	n := int64(0)
	for i := 0; i < 1000; i++ {
		err = dbi.DoSQL("INSERT INTO demot VALUES (now, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", i)
		if err != nil {
			panic(err)
		}
		n += dbi.Affected
		time.Sleep(1 * time.Millisecond)
	}
	et := time.Now()
	if n != 1000 {
		t.Errorf("1000 exptected, %d rows found", n)
	}
	if et.Sub(st).Seconds() > 10 {
		t.Errorf("total second to insert 1000 rows: %6.6f which is too slow", et.Sub(st).Seconds())
	}

	st = et
	lists := make([]map[string]interface{}, 0)
	n = int64(0)
	for i := 0; i < 10000; i++ {
		err = dbi.SelectSQL(&lists,
			"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
		if err != nil {
			panic(err)
		}
		n += dbi.Affected
	}
	et = time.Now()
	if n != 10000 {
		t.Errorf("10000 exptected, %d rows found", n)
	}
	if et.Sub(st).Seconds() > 10 {
		t.Errorf("total second to query 10000 times: %6.6f which is too slow", et.Sub(st).Seconds())
	}

	st = et
	lists = make([]map[string]interface{}, 0)
	n = int64(0)
	for i := 0; i < 10000; i++ {
		err = dbi.QuerySQL(&lists,
			"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
		if err != nil {
			panic(err)
		}
		n += dbi.Affected
	}
	et = time.Now()
	if n != 10000 {
		t.Errorf("10000 exptected, %d rows found", n)
	}
	if et.Sub(st).Seconds() > 10 {
		t.Errorf("total second to query 10000 times: %6.6f which is too slow", et.Sub(st).Seconds())
	}

	db.Close()
}

func TestShort(t *testing.T) {
	dbname := "demodb"
	c := newconf("config.json")
	db, err := sql.Open(c.Db_type, c.Dsn_1)
	if err != nil {
		panic(err)
	}
	dbi := &DBI{Db: db}

	err = dbi.ExecSQL(`drop database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`create database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`use ` + dbname)
	if err != nil {
		panic(err)
	}
	tb := "demot"
	err = dbi.ExecSQL("create table " + tb + " (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)")
	if err != nil {
		panic(err)
	}

	n := int64(0)
	for i := 0; i < 10; i++ {
		err = dbi.DoSQL("INSERT INTO demot VALUES (now, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", i)
		if err != nil {
			panic(err)
		}
		n += dbi.Affected
		time.Sleep(1 * time.Millisecond)
	}
	err = dbi.DoSQL("INSERT INTO demot VALUES (now, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", 20000)
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Millisecond)
	err = dbi.DoSQL("INSERT INTO demot (ts,id,name) VALUES (now, ?, 'beijing')", 30000)
	if err != nil {
		panic(err)
	}

	lists := make([]map[string]interface{}, 0)
	types := []string{"string", "int", "string", "int8", "bool", "string", "float32", "float64"}
	err = dbi.SelectSQLType(&lists, types,
		"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
	if err != nil {
		panic(err)
	}
	for i, item := range lists {
		if i > 0 {
			continue
		}
		if fmt.Sprintf("%T", item["ts"]) != "string" ||
			fmt.Sprintf("%T", item["id"]) != "int" ||
			fmt.Sprintf("%T", item["name"]) != "string" ||
			fmt.Sprintf("%T", item["len"]) != "int8" ||
			fmt.Sprintf("%T", item["flag"]) != "bool" ||
			fmt.Sprintf("%T", item["notes"]) != "string" ||
			fmt.Sprintf("%T", item["fv"]) != "float32" ||
			fmt.Sprintf("%T", item["dv"]) != "float64" {

			t.Errorf("type string exptected for ts: %T", item["ts"])
			t.Errorf("type int exptected for ts: %T", item["id"])
			t.Errorf("type string exptected for ts: %T", item["name"])
			t.Errorf("type int8 exptected for ts: %T", item["len"])
			t.Errorf("type bool exptected for ts: %T", item["flag"])
			t.Errorf("type string exptected for ts: %T", item["notes"])
			t.Errorf("type float32 exptected for ts: %T", item["fv"])
			t.Errorf("type float64 exptected for ts: %T", item["dv"])
		}
	}

	nlen := len(lists)
	l0 := lists[0]
	ln := lists[nlen-1]
	if l0["dv"] != float64(456.789) ||
		l0["fv"] != float32(789.123) ||
		l0["id"] != int(0) ||
		l0["len"] != int8(111) ||
		l0["name"] != "beijing" ||
		l0["notes"] != "abcdefgh" {
		t.Errorf("wrong first row: %#v", l0)
	}
	if ln["dv"] != nil ||
		ln["fv"] != nil ||
		ln["id"] != int(30000) ||
		ln["len"] != nil ||
		ln["name"] != "beijing" ||
		ln["notes"] != nil {
		t.Errorf("wrong last row: %#v", ln)
	}

	lists = make([]map[string]interface{}, 0)
	err = dbi.SelectSQL(&lists,
		"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
	if err != nil {
		panic(err)
	}

	for i, item := range lists {
		if i > 0 {
			continue
		}
		if fmt.Sprintf("%T", item["ts"]) != "string" ||
			fmt.Sprintf("%T", item["id"]) != "int" ||
			fmt.Sprintf("%T", item["name"]) != "string" ||
			fmt.Sprintf("%T", item["len"]) != "int" ||
			fmt.Sprintf("%T", item["flag"]) != "bool" ||
			fmt.Sprintf("%T", item["notes"]) != "string" ||
			fmt.Sprintf("%T", item["fv"]) != "float32" ||
			fmt.Sprintf("%T", item["dv"]) != "float64" {

			t.Errorf("type string exptected for ts: %T", item["ts"])
			t.Errorf("type int exptected for ts: %T", item["id"])
			t.Errorf("type string exptected for ts: %T", item["name"])
			t.Errorf("type int exptected for ts: %T", item["len"])
			t.Errorf("type bool exptected for ts: %T", item["flag"])
			t.Errorf("type string exptected for ts: %T", item["notes"])
			t.Errorf("type float32 exptected for ts: %T", item["fv"])
			t.Errorf("type float64 exptected for ts: %T", item["dv"])
		}
	}

	nlen = len(lists)
	l0 = lists[0]
	ln = lists[nlen-1]
	if l0["dv"] != float64(456.789) ||
		l0["fv"] != float32(789.123) ||
		l0["id"] != int(0) ||
		l0["len"] != int(111) ||
		l0["name"] != "beijing" ||
		l0["notes"] != "abcdefgh" {
		t.Errorf("wrong first row: %#v", l0)
	}
	if ln["dv"] != nil ||
		ln["fv"] != nil ||
		ln["id"] != int(30000) ||
		ln["len"] != nil ||
		ln["name"] != "beijing" ||
		ln["notes"] != nil {
		t.Errorf("wrong last row: %#v", ln)
	}
}

func TestInt(t *testing.T) {
	dbname := "demodb"
	c := newconf("config.json")
	db, err := sql.Open(c.Db_type, c.Dsn_2)
	if err != nil {
		panic(err)
	}
	dbi := &DBI{Db: db}

	err = dbi.ExecSQL(`drop database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`create database ` + dbname)
	if err != nil {
		panic(err)
	}
	err = dbi.ExecSQL(`use ` + dbname)
	if err != nil {
		panic(err)
	}
	tb := "demot"
	err = dbi.ExecSQL("create table " + tb + " (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)")
	if err != nil {
		panic(err)
	}

	n := int64(0)
	for i := 0; i < 10; i++ {
		//err = dbi.DoSQL("INSERT INTO demot VALUES (?, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", time.Now().UnixNano(), i)
		err = dbi.DoSQL("INSERT INTO demot VALUES (?, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", time.Now().UnixNano()/int64(time.Millisecond), i)
		if err != nil {
			panic(err)
		}
		n += dbi.Affected
		time.Sleep(1 * time.Millisecond)
	}
	//err = dbi.DoSQL("INSERT INTO demot VALUES (?, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", time.Now().UnixNano(), 20000)
	err = dbi.DoSQL("INSERT INTO demot VALUES (?, ?, 'beijing', 111, true, 'abcdefgh', 789.123, 456.789)", time.Now().UnixNano()/int64(time.Millisecond), 20000)
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Millisecond)
	// now is in milliseconds automatically
	err = dbi.DoSQL("INSERT INTO demot (ts,id,name) VALUES (now, ?, ?)", 30000, "'beijing'")
	if err != nil {
		panic(err)
	}

	lists := make([]map[string]interface{}, 0)
	types := []string{"int64", "int", "string", "int8", "bool", "string", "float32", "float64"}
	err = dbi.SelectSQLType(&lists, types,
		"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
	if err != nil {
		panic(err)
	}
	for i, item := range lists {
		if i > 0 {
			continue
		}
		if fmt.Sprintf("%T", item["ts"]) != "int64" ||
			fmt.Sprintf("%T", item["id"]) != "int" ||
			fmt.Sprintf("%T", item["name"]) != "string" ||
			fmt.Sprintf("%T", item["len"]) != "int8" ||
			fmt.Sprintf("%T", item["flag"]) != "bool" ||
			fmt.Sprintf("%T", item["notes"]) != "string" ||
			fmt.Sprintf("%T", item["fv"]) != "float32" ||
			fmt.Sprintf("%T", item["dv"]) != "float64" {

			t.Errorf("type int64 exptected for ts: %T", item["ts"])
			t.Errorf("type int exptected for ts: %T", item["id"])
			t.Errorf("type string exptected for ts: %T", item["name"])
			t.Errorf("type int8 exptected for ts: %T", item["len"])
			t.Errorf("type bool exptected for ts: %T", item["flag"])
			t.Errorf("type string exptected for ts: %T", item["notes"])
			t.Errorf("type float32 exptected for ts: %T", item["fv"])
			t.Errorf("type float64 exptected for ts: %T", item["dv"])
		}
	}

	nlen := len(lists)
	l0 := lists[0]
	ln := lists[nlen-1]
	if l0["dv"] != float64(456.789) ||
		l0["fv"] != float32(789.123) ||
		l0["id"] != int(0) ||
		l0["len"] != int8(111) ||
		l0["name"] != "beijing" ||
		l0["notes"] != "abcdefgh" {
		t.Errorf("wrong first row: %#v", l0)
	}
	if ln["dv"] != nil ||
		ln["fv"] != nil ||
		ln["id"] != int(30000) ||
		ln["len"] != nil ||
		ln["name"] != "beijing" ||
		ln["notes"] != nil {
		t.Errorf("wrong last row: %#v", ln)
	}

	lists = make([]map[string]interface{}, 0)
	err = dbi.SelectSQL(&lists,
		"SELECT ts, id, name, len, flag, notes, fv, dv FROM demot LIMIT 20")
	if err != nil {
		panic(err)
	}

	for i, item := range lists {
		if i > 0 {
			continue
		}
		if fmt.Sprintf("%T", item["ts"]) != "int64" ||
			fmt.Sprintf("%T", item["id"]) != "int" ||
			fmt.Sprintf("%T", item["name"]) != "string" ||
			fmt.Sprintf("%T", item["len"]) != "int" ||
			fmt.Sprintf("%T", item["flag"]) != "bool" ||
			fmt.Sprintf("%T", item["notes"]) != "string" ||
			fmt.Sprintf("%T", item["fv"]) != "float32" ||
			fmt.Sprintf("%T", item["dv"]) != "float64" {

			t.Errorf("type int64 exptected for ts: %T", item["ts"])
			t.Errorf("type int exptected for ts: %T", item["id"])
			t.Errorf("type string exptected for ts: %T", item["name"])
			t.Errorf("type int exptected for ts: %T", item["len"])
			t.Errorf("type bool exptected for ts: %T", item["flag"])
			t.Errorf("type string exptected for ts: %T", item["notes"])
			t.Errorf("type float32 exptected for ts: %T", item["fv"])
			t.Errorf("type float64 exptected for ts: %T", item["dv"])
		}
	}

	nlen = len(lists)
	l0 = lists[0]
	ln = lists[nlen-1]
	//t.Errorf("%T:%v", l0["ts"], l0["ts"])
	// the following record used now function
	//t.Errorf("%T:%v", ln["ts"], ln["ts"])
	if l0["dv"] != float64(456.789) ||
		l0["fv"] != float32(789.123) ||
		l0["id"] != int(0) ||
		l0["len"] != int(111) ||
		l0["name"] != "beijing" ||
		l0["notes"] != "abcdefgh" {
		t.Errorf("wrong first row: %#v", l0)
	}
	if ln["dv"] != nil ||
		ln["fv"] != nil ||
		ln["id"] != int(30000) ||
		ln["len"] != nil ||
		ln["name"] != "beijing" ||
		ln["notes"] != nil {
		t.Errorf("wrong last row: %#v", ln)
	}
}
