package taodbi

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"

	_ "github.com/taosdata/driver-go/taosSql"
)

type conf struct {
	DbType string `json:"DbType"`
	Dsn1   string `json:"Dsn1"`
	Dsn2   string `json:"Dsn2"`
}

func newconf(filename string) *conf {
	parsed := new(conf)
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(content, parsed)
	if err != nil {
		panic(err)
	}
	return parsed
}

func getString(filename string) []byte {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return content
}

func open(ds string) (*sql.DB, error) {
	return sql.Open("taosSql", ds)
}
