package taodbi

import (
	"strings"
	"testing"
)

func TestTableStatusColumn(t *testing.T) {
	table := &Table{InsertPars:[]string{"id","status"}, ForeignKey:"id"}
	if table.statusColumn() != "status" {
		t.Errorf("status wanted, got: %s", table.statusColumn())
	}
}

func TestTableStr(t *testing.T) {
	selectPar := "firstname"
	sql, labels, types := selectType(selectPar)
	if sql != "firstname" {
		t.Errorf("%s wanted", sql)
	}
	if labels[0] != "firstname" {
		t.Errorf("%s wanted", labels[0])
	}
	if types != nil {
		t.Errorf("%v wanted", types)
	}

	selectPars := []string{"firstname", "lastname", "id"}
	sql, labels, types = selectType(selectPars)
	if sql != "firstname, lastname, id" {
		t.Errorf("%s wanted", sql)
	}
	if labels[0] != "firstname" {
		t.Errorf("%s wanted", labels[0])
	}
	if types != nil {
		t.Errorf("%v wanted", types)
	}

	selectHash := map[string]string{"firstname": "First", "lastname": "Last", "id": "ID"}
	sql, labels, types = selectType(selectHash)
	if !strings.Contains(sql, "firstname") {
		t.Errorf("%s wanted", sql)
	}
	if types != nil {
		t.Errorf("%s wanted", types)
	}
	if !grep(labels, "First") {
		t.Errorf("%s wanted", labels)
	}

	extra := map[string]interface{}{"firstname": "Peter"}
	sql, c := selectCondition(extra)
	if sql != "(firstname=?)" {
		t.Errorf("%s wanted", sql)
	}
	if c[0].(string) != "Peter" {
		t.Errorf("%s wanted", c[0].(string))
	}

	sql, c = selectCondition(extra)
	if sql != "(firstname=?)" {
		t.Errorf("%s wanted", sql)
	}
	if c[0].(string) != "Peter" {
		t.Errorf("%s wanted", c[0].(string))
	}

	extra["lastname"] = "Marcus"
	extra["id"] = []interface{}{1,2,3,4}
	sql, c = selectCondition(extra)
	if !(strings.Contains(sql, "(firstname=?)") &&
		strings.Contains(sql, "(id IN (?,?,?,?))") &&
		strings.Contains(sql, "(lastname=?)")) {
		t.Errorf("%s wanted", sql)
	}
	if len(c) != 6 {
		t.Errorf("%v wanted", c)
	}

/*
	crud := new(Table)
	crud.ForeignKeys = []string{"user_id", "edu_id"}
	ids := []interface{}{[]interface{}{11, 22}, []interface{}{33, 44, 55}}
	s, arr := crud.singleFKCondition(ids, extra)
	if !(strings.Contains(s, "user_id IN (?,?)") &&
		strings.Contains(s, "edu_id IN (?,?,?)") &&
		strings.Contains(s, "id IN (?,?,?,?)") &&
		strings.Contains(s, "(firstname =?)") &&
		strings.Contains(s, "(lastname =?)")) {
		t.Errorf("%s wanted", s)
	}
	if len(arr) != 11 {
		t.Errorf("%v wanted", ids)
		t.Errorf("%v wanted", extra)
		t.Errorf("%v wanted", s)
		t.Errorf("%v wanted", arr)
	}
*/
}
