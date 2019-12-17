package taodbi

import (
    "testing"
//	"time"
//	"strconv"
//	"math/rand"
    "database/sql"
    _ "taosSql"
)

func TestModel(t *testing.T) {
    c := newconf("config.json")
    db, err := sql.Open(c.Db_type, c.Dsn_2)
    if err != nil { panic(err) }
	model, err := NewSmodel(getString("ms.json"))
    if err != nil { panic(err) }
	model.Db = db

	err = model.ExecSQL(`drop table if exists stesting`)
	if err != nil { panic(err) }
	err = model.ExecSQL(`CREATE TABLE stesting (id timestamp, x binary(8), y binary(8), z binary(8)) TAGS (pubid int, location binary(8))`)
	if err != nil { panic(err) }

	model.ARGS = map[string]interface{}{"pubid":123, "location":"la"}
	err = model.CreateTable()
	if err != nil { panic(err) }
	err = model.CreateTable()
	if err != nil { panic(err) }
	db.Close()
}
