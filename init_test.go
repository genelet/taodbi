package taodbi

import (
	"encoding/json"
	"io/ioutil"
)

type conf struct {
	Db_type	string `json:"db_type"`
	Dsn_1 string `json:"dsn_1"`
	Dsn_2 string `json:"dsn_2"`
}
func newconf(filename string) *conf {
    parsed := new(conf)
    content, err := ioutil.ReadFile(filename)
    if err != nil { panic(err) }
    err = json.Unmarshal(content, parsed)
    if err != nil { panic(err) }
	return parsed
}

func getString(filename string) []byte {
    content, err := ioutil.ReadFile(filename)
    if err != nil { panic(err) }
	return content
}
