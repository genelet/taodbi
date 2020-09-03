# taodbi
An abstract interface class to access the big data system TDengine in GO. Check *godoc* from [here](https://godoc.org/github.com/genelet/taodbi)

[![GoDoc](https://godoc.org/github.com/genelet/taodbi?status.svg)](https://godoc.org/github.com/genelet/taodbi)

[TDengine](https://github.com/taosdata/TDengine) is a very fast open-source database system. It comes with a [GO connector](https://github.com/taosdata/driver-go). This _taodbi_ GO package provides a set of *abstract* classes to access it, and to simulate *Update* and *Delete* verbs as in relational database system. 

There are three levels of usages:

- _Basic_: operating on raw SQL statements and stored procedures.
- _Model_: operating on specific table and fulfilling CRUD actions, as *Model* in MVC pattern.
  - *R-Model*: simulating full CRUD or RESTful actions
  - *S-Model*: operating on *TDengine*'s own *S-Table* 
- _Schema_: operating on whole database schema and fulfilling RESTful and GraphQL actions.


### Installation

```
$ go get -u github.com/genelet/taodbi
```

### Termilogy

The names of arguments passed in functions or methods in this package are defined as follows, if not specifically explained:
Name | Type | IN/OUT | Where | Meaning
---- | ---- | ------ | ----- | -------
*args* | `...interface{}` | IN | `DBI` | single-valued interface slice, possibly empty
*args* | `map[string]interface{}` | IN | `Model` | via SetArgs() to set input data
*args* | `map[string]interface{}` | IN | `Schema` | input data passing to Run()
*extra* | `map[string]interface{}` | IN | `Model`,`Schema` | WHERE constraints; single value - EQUAL,  multi value - IN
*lists* | `[]map[string]interface{}` | OUT | all | output as slice of rows; each row is a map.
*res* | `map[string]interface{}` | OUT | `DBI` | output for one row

<br /><br />

## Chapter 1. BASIC USAGE

### 1.1  Type _DBI_

The `DBI` type simply embeds the standard SQL handle.

```go
package godbi

type DBI struct {
    *sql.DB          // Note this is the pointer to the handle
    LastID    int64  // read only, saves the last inserted id
    Affected  int64  // read only, saves the affected rows
}

```

#### 1.1.1) Create a new handle

```go
dbi := &DBI{DB: the_standard_sql_handle}
```

#### 1.1.2) Example

In this example, we create a MySQL handle using database credentials in the environment; then create a new table _letters_ and add 3 rows. We query the data using `SelectSQL` and put the result into `lists` as slice of maps.

<details>
    <summary>Click for Sample 1</summary>
    <p>

```go
package main

import (
    "log"
    "os"
    "database/sql"
    "github.com/genelet/taodbi"
    _ "github.com/taosdata/driver-go/taosSql"
)

func main() {
    db, err := sql.Open("taosSql", "root:taosdata@/tcp(127.0.0.1:0)/")
    if err != nil { panic(err) }
    defer db.Close()

    dbi := &taodbi.DBI{DB: db}

    err = dbi.DoSQL(`CREATE DATABASE IF NOT EXISTS mydbi precision "us"`)
    if err != nil { panic(err) }
    err = dbi.DoSQL(`USE mydbi`)
    if err != nil { panic(err) }
    err = dbi.DoSQL(`DROP TABLE IF EXISTS mytable`)
    if err != nil { panic(err) }
    err = dbi.DoSQL(`CREATE TABLE mytable 
(ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)`)
    if err != nil { panic(err) }
    err = dbi.DoSQL(`INSERT INTO mytable (ts, id, name, len, flag, notes, fv, dv)
VALUES (now, ?, ?, 30, true, 'abcdefgh', 789.123, 456.789)`, 1234, `company`)
    if err != nil { panic(err) }
    lists := make([]map[string]interface{},0)
    err = dbi.SelectSQL(&lists,
`SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
    if err != nil { panic(err) }
    
    log.Printf("%v", lists)

    err = dbi.DoSQL(`DROP DATABASE IF EXISTS mydbi`)
    if err != nil { panic(err) }

    os.Exit(0)
}
```
Running this example will report something like
```
[map[flag:true fv:789.123 id:1234 len:30 name:company ts:2020-07-19 09:07:48.341270]]
```

</p>
</details>

<br /><br />

### 1.2  Execution `DoSQL`

```go
func (*DBI) DoSQL  (query string, args ...interface{}) error
```

Similar to SQL's `Exec`, `DoSQL` executes *Do*-type (e.g. _INSERT_ or _UPDATE_) queries. It runs a prepared statement and may be safe for concurrent use by multiple goroutines.

For all functions in this package, the returned value is always `error` which should be checked to assert if the execution is successful.

<br /><br />

### 1.3   _SELECT_ Queries

#### 1.3.1)  `SelectSQL`

```go
func (*DBI) SelectSQL(lists *[]map[string]interface{}, query string, args ...interface{}) error
```

Run the *SELECT*-type query and put the result into `lists`, a slice of column name-value maps. The data types of the column are determined dynamically by the generic SQL handle.

<details>
    <summary>Click for example</summary>
    <p>

```go
lists := make([]map[string]interface{})
err = dbi.DoSQL(&lists,
    `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```

will select all rows with *id=1234*.

```json
    {"ts":"2019-12-15 01:01:01", "id":1234, "name":"company", "len":30, "flag":true, "fv":789.123},
    ....
```

</p>
</details>

`SelectSQL` runs a prepared statement.

#### 1.3.2) `SelectSQLType`

```go
func (*DBI) SelectSQLType(lists *[]map[string]interface{}, typeLabels []string, query string, args ...interface{}) error
```

They differ from the above `SelectSQL` by specifying the data types. While the generic handle could correctly figure out them in most cases, it occasionally fails because there is no exact matching between SQL types and GOLANG types.

The following example assigns _string_, _int_, _string_, _int8_, _bool_ and _float32_ to the corresponding columns:

```go
err = dbi.SelectSQLType(&lists, []string{"string", "int", "string", "int8", "bool", "float32},
    `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```

#### 1.3.3) `SelectSQLLabel`

```go
func (*DBI) SelectSQLLabel(lists *[]map[string]interface{}, selectLabels []string, query string, args ...interface{}) error
```

They differ from the above `SelectSQL` by renaming the default column names to `selectLabels`.

<details>
    <summary>Click for example</summary>
    <p>

```go
lists := make([]map[string]interface{})
err = dbi.querySQLLabel(&lists, []string{"time stamp", "record ID", "recorder name", "length", "flag", "values"},
    `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```

The result has the renamed keys:

```json
    {"time stamp":"2019-12-15 01:01:01", "record ID":1234, "recorder name":"company", "length":30, "flag":true, "values":789.123},
```

</p>
</details>

#### 1.3.4) `SelectSQlTypeLabel`

```go
func (*DBI) SelectSQLTypeLabel(lists *[]map[string]interface{}, typeLabels []string, selectLabels []string, query string, args ...interface{}) error
```

These functions re-assign both data types and column names in the queries.

<br /><br />

### 1.4  Query Single Row

In some cases we know there is only one row from a query.

#### 1.4.1) `GetSQLLable`

```go
func (*DBI) GetSQLLabel(res map[string]interface{}, query string, selectLabels []string, args ...interface{}) error
```

which is similar to `SelectSQLLabel` but has only single output to `res`.

#### 1.4.2) `GetArgs`

```go
func (*DBI) GetArgs(res map[string]interface{}, query string, args ...interface{}) error
```

which is similar to `SelectSQL` but has only single output to `res` which uses type *map[string]interface{}*. This function will be used mainly in web applications, where HTTP request data are expressed in `map[string]interface{}`.

<br /><br />

### 1.5) Function *Quote*

This static function escape a string for unsafe characters *[';]*. You don't need to call it in the above *ExecSQL* and *QuerySQL* because we already do it.

<br /><br />

## Chapter 2. MODEL USAGE

*taodbi* allows us to construct *model* as in the MVC Pattern in web applications, and to build RESTful API easily. The CRUD verbs on table are defined to be:
C | R | U | D
---- | ---- | ---- | ----
create a new row | read all rows, or read one row | update a row | delete a row

The RESTful web actions are associated with the CRUD verbs as follows:

<details>
    <summary>Click for RESTful vs CRUD</summary>
    <p>

HTTP METHOD | Web URL | CRUD | Function | Available
----------- | ------- | ---- | -------- | ---------
GET         | webHandler | R All | Topics | model, rmodel & smodel
GET         | webHandler/ID | R One | Edit | model, rmodel & smodel
POST        | webHandler | C | Insert | model, rmodel & smodel
PUT         | webHandler | U | Update | rmodel
PATCH       | webHandler | NA | Insupd | rmodel
DELETE      | webHandler | D | Delete | rmodel

</p>
</details>

As a time series big data system, *TDengine* does not implement *Update* nor *Delete* verbs. This package simulates them in *R-Model*. Therefore, we have
three types of *Model*:
- *Model*, operating on TDengine tables with only *R* and *C* verbs
- *Rmodel*, operating on simulated TDengine tables with full *CRUD* verbs
- *Smodel*, operating on TDengine's super tables with only *R* nd *C* verbs

<br /><br />

### 2.1  Type *Table*

*taodbi* uses JSON to express the CRUD fields and logic. There should be one, and
only one, JSON (file or string) assigned to each database table. The JSON is designed only once. In case of any change in the business logic, we can modify it, which is much cleaner and easier to do than changing program code, as in ORM.

Here is the `Table` type:

```go
    CurrentTable   string             `json:"current_table,omitempty"`   // the current table name
    CurrentKey     string             `json:"current_key,omitempty"`     // the single primary key of the table
    ForeignKey     string             `json:"foreign_key,omitempty"`     // optional, a FK-like column 
    InsertPars     []string           `json:"insert_pars,omitempty"`     // columns to insert in C
    InsupdPars     []string           `json:"insupd_pars,omitempty"`     // unique columns in PATCH
    EditPars       []interface{}      `json:"edit_pars,omitempty"`       // columns to query in R (one)
    EditHash   map[string]interface{} `json:"edit_hash,omitempty"`       // R(a) with specific types and labels
    TopicsPars     []interface{}      `json:"topics_pars,omitempty"`     // columns to query in R (all)
    TopicsHash map[string]interface{} `json:"topics_hash,omitempty"`     // R(a) with specific types and labels
    TotalForce     int                `json:"total_force,omitempty"`     // if to calculate total counts in R(a)

    Nextpages      map[string][]*Page `json:"nextpages,omitempty"`       // to call other models' verbs

    // The following fields are just variable names to pass in a web request,
    // default to themselves. e.g. "empties" for "Empties", "maxpageno" for Maxpageno etc.
    Empties        string             `json:"empties,omitempty"`         // columns are updated to NULL if no input
    Fields         string             `json:"fields,omitempty"`          // use this smaller set of columns in R
    // the following fields are for pagination.
    Totalno        string             `json:"totalno,omitempty"`         // total item no.
    Rowcount       string             `json:"rowcount,omitempty"`        // counts per page
    Pageno         string             `json:"pageno,omitempty"`          // current page no.
    Sortreverse    string             `json:"sortreverse,omitempty"`     // if reverse sorting
    Sortby         string             `json:"sortby,omitempty"`          // sorting column
}
```

And here is explanation of the fields:

<details>
    <summary>Click to Show Fields in Model</summary>
    <p>

Field in Model | JSON variable | Database Table
-------------- | ------------- | --------------
CurrentTable | current_table | the current table name
CurrentKey | current_key | the single primary key of the table
ForeignKey | foreign_key | optional, a foreign-like column, explained below 
InsertPars     | insert_pars | columns to insert in C
InsupdPars     | insupd_pars | unique columns in PATCH
EditPars       | edit_pars | columns to query in R (one)
TopicsPars     | topics_pars | columns to query in R (all)
TotalForce     | total_force | if to calculate total counts in R (all)

</p>
</details>

#### 2.1.1) *Read* with specific types and/or names

While in most cases we *Read* by simple column slice, i.e. *EditPars* & *TopicsPars*,
occasionally we need specific names and types in output. Here is what *godbi* will do
in case of existence of *EditHash* or/and *TopicsHash*.

<details>
    <summary>Click to show <em>EditPars</em>, <em>EditHash</em>, <em>TopicsPars</em> and <em>TopicsHash</em></summary>
    <p>

interface | variable | column names
--------- | -------- | ------------
 *[]string{name}* | EditPars, TopicsPars | just a list of column names
 *[][2]string{name, type}* | EditPars, TopicsPars | column names and their data types
 *map[string]string{name: label}* | EditHash, TopicsHash | rename the column names by labels
 *map[string][2]string{name: label, type}* | EditHash, TopicsHash | rename and use the specific types

</p>
</details>

#### 2.1.2) Pagination

We have define a few variable names whose values can be passed in input data, to make *Read All* in pagination.

First, use `TotalForce` to define how to calculate the total row count.

<details>
    <summary>Click for meaning of *TotalForce*</summary>
    <p>

Value | Meaning
----- | -------
<-1  | use ABS(TotalForce) as the total count
-1   | always calculate the total count
0    | don't calculate the total count
&gt; 0  | calculate only if the total count is not passed in `args`

</p>
</details>

If variable `rowcount` (*number of records per page*) is set in input, and field `TotalForce` is not 0, then pagination will be triggered. The total count and total pages will be calculated and put back in variable names `totalno` and `maxpageno`. For consecutive requests, we should attach values of *pageno*, *totalno* and *rowcount* to get the
right page back.

By combining *TopicsHash*, *CurrentTables* and the pagination variables, we can build up quite sophisticated SQLs for most queries.

#### 2.1.3) Definition of *Next Pages*

As in GraphQL and gRCP, *godbi* allows an action to trigger multiple actions on other models. To what actions
on other models will get triggered, define *Nextpages* in *Table*.

Here is type *Page*:

```go
type Page struct {
    Model      string            `json:"model"`                 // name of the next model to call  
    Action     string            `json:"action"`                // action name of the next model
    RelateItem map[string]string `json:"relate_item,omitempty"` // column name mapped to that of the next model
    Manual     map[string]string `json:"manual,omitempty"`      // manually assign these constraints
}
```

Assume there are two tables, one for family and the other for children, corresponding to two models `ta` and `tb` respectively.

When we *GET* the family name, we'd like to show all children under the family name as well. Technically, it means that running `Topics` on `ta` will trigger `Topics` on `tb`, constrained by the association of family's ID in both the tables. The same is true for `Edit` and `Insert`. So for the family model, its `Nextpages` will look like

<details>
    <summary>Click to show the JSON string</summary>
    <p>

```json
{
    "insert" : [
        {"model":"tb", "action":"insert", "relate_item":{"id":"id"}}
    ],
    "edit" : [
        {"model":"tb", "action":"topics", "relate_item":{"id":"id"}}
    ],
    "topics" : [
        {"model":"tb", "action":"topics", "relate_item":{"id":"id"}}
    ]
}
```

</p>
</details>

Parsing it will result in `map[string][]*Page`. *godbi* will run all the next pages automatically in chain.

<br /><br />

### 2.2  Interface *Navigate*

All *Model*, "Rmodel* and *Smodel* are implementations of interface *Navigate*.

```go
type Navigate interface {
    SetArgs(map[string]interface{})                            // set http request data
    SetDB(*sql.DB)                                 // set the database handle
    GetAction(string)   func(...map[string]interface{}) error  // get function by action name
    GetLists()          []map[string]interface{}   // get result after an action
}
```

#### 2.2.1) Set Database Handle and Input Data

Use

```go
func (*Navigate) SetDB(db *sql.DB)
func (*Navigate) SetArgs(args map[string]interface{})
```

to set database handle `db`, and input data `args`. The input data is of type *map[string]interface{}*.
In web applications, this is *Form* from http request in `net/http`.


#### 2.2.2) Returning Data

After we have run an action on the model, we can retrieve data using

```go
(*Model) GetLists()
```

The closure associated with the action name can be get back:

```go
(*Model) GetAction(name string) func(...map[string]interface{}) error
```

<br /><br />


### 2.3  Type *Model*

```go
type Model struct {
    DBI
    Table
    Navigate                                        // interface has methods to implement
    Actions   map[string]func(...map[string]interface{}) error  // action name to closure map
    Updated
```

where `Actions` is an action name to action closure map.

#### 2.3.1) Constructor `NewModel`

A `Model` instance can be parsed from JSON file on disk:

```go
func NewModel(filename string) (*Model, error)
```

where `filename` is the file name.

#### 2.3.2) Optional Constraints

For all RESTful methods of *Model*, we have option to put a data structure, named `extra` and of type `map[string]interface{}`, to constrain the *WHERE* statement. Currently we have supported 3 cases:

<details>
    <summary>Click to show *extra*</summary>
    <p>

key in `extra` | meaning
--------------------------- | -------
key has only one value | an EQUAL constraint
key has multiple values | an IN constraint
key is named *_gsql* | a raw SQL statement
among multiple keys | AND conditions.

</p>
</details>


#### 2.3.3) For Http METHOD: GET (read all)

```go
func (*Model) Topics(extra ...map[string]interface{}) error
```

#### 2.3.4) For Http METHOD: GET (read one)

```go
func (*Model) Edit(extra ...map[string]interface{}) error
```

#### 2.3.5) For Http METHOD: POST (create)

```go
func (*Model) Insert(extra ...map[string]interface{}) error
```

It inserts a new row using the input data. If `extra` is passed in, it will override the input data.

#### 2.3.6）Example

<details>
    <summary>Click for example to run RESTful actions</summary>
    <p>

```go
package main

import (
    "log"
    "os"
    "database/sql"
    "github.com/genelet/godbi"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    dbUser := os.Getenv("DBUSER")
    dbPass := os.Getenv("DBPASS")
    dbName := os.Getenv("DBNAME")
    db, err := sql.Open("mysql", dbUser + ":" + dbPass + "@/" + dbName)
    if err != nil { panic(err) }
    defer db.Close()

    model := new(godbi.Model)
    model.CurrentTable = "testing"
    model.Sortby        ="sortby"
    model.Sortreverse   ="sortreverse"
    model.Pageno        ="pageno"
    model.Rowcount      ="rowcount"
    model.Totalno       ="totalno"
    model.Maxpageno     ="max_pageno"
    model.Fields        ="fields"
    model.Empties       ="empties"

    db.Exec(`DROP TABLE IF EXISTS testing`)
    db.Exec(`CREATE TABLE testing (id int auto_increment, x varchar(255), y varchar(255), primary key (id))`)

    args := make(map[string]interface{})
    model.SetDB(db)
    model.SetArgs(args)

    model.CurrentKey    = "id"
    model.CurrentIDAuto = "id"
    model.InsertPars    = []string{     "x","y"}
    model.TopicsPars    = []string{"id","x","y"}
    model.UpdatePars    = []string{"id","x","y"}
    model.EditPars      = []string{"id","x","y"}

    args["x"] = []string{"a"}
    args["y"] = []string{"b"}
    if err := model.Insert(); err != nil { panic(err) }
    log.Println(model.LastID)

    args["x"] = []string{"c"}
    args["y"] = []string{"d"}
    if err := model.Insert(); err != nil { panic(err) }
    log.Println(model.LastID)

    if err := model.Topics(); err != nil { panic(err) }
    log.Println(model.GetLists())

    args.Set("id","2")
    args["x"] = []string{"c"}
    args["y"] = []string{"z"}
    if err := model.Update(); err != nil { panic(err) }
    if err := model.Edit(); err != nil { panic(err) }
    log.Println(model.GetLists())

    os.Exit(0)
}
```

Running the program will result in

```bash
1
2
[map[id:1 x:a y:b] map[id:2 x:c y:d]]
[map[id:2 x:c y:z]]
```

</p>
</details>

<br /><br />

### 2.4 Type *Rmodel*

```go
type Rmodel struct {
    Model

    ProfileTable *Model `json:"profile_table,omitempty"` // non unique fields
    StatusTable  *Model `json:"status_table,omitempty"`  // gmark_delete
}
```

#### 2.4.1) Build Simulated RESTful Tables

#### 2.4.2) Constructor `NewModel`

A `Model` instance can be parsed from JSON file on disk:

```go
func NewModel(filename string) (*Model, error)
```

where `filename` is the file name.

#### 2.4.3) For Http METHOD: GET (read all)

```go
func (*Model) Topics(extra ...map[string]interface{}) error
```

#### 2.4.4) For Http METHOD: GET (read one)

```go
func (*Model) Edit(extra ...map[string]interface{}) error
```

#### 2.4.5) For Http METHOD: POST (create)

```go
func (*Model) Insert(extra ...map[string]interface{}) error
```

It inserts a new row using the input data. If `extra` is passed in, it will override the input data.

#### 2.4.6) Http METHOD: PUT (update)

```go
func (*Model) Update(extra ...map[string]interface{}) error
```

It updates a row using the input data, constrained by `extra`.

#### 2.4.7) Http METHOD: PATCH (insupd)

```go
func (*Model) Insupd(extra ...map[string]interface{}) error
```

It inserts or updates a row using the input data, constrained optionally by `extra`.

#### 2.4.8) Http METHOD: DELETE

```go
func (*Model) Delete(extra ...map[string]interface{}) error
```

It rows constrained by `extra`. For this function, the input data will NOT be used.

#### 2.4.9）Example

<details>
    <summary>Click for example to run RESTful actions</summary>
    <p>

```go
package main

import (
    "log"
    "os"
    "database/sql"
    "github.com/genelet/godbi"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    dbUser := os.Getenv("DBUSER")
    dbPass := os.Getenv("DBPASS")
    dbName := os.Getenv("DBNAME")
    db, err := sql.Open("mysql", dbUser + ":" + dbPass + "@/" + dbName)
    if err != nil { panic(err) }
    defer db.Close()

    model := new(godbi.Model)
    model.CurrentTable = "testing"
    model.Sortby        ="sortby"
    model.Sortreverse   ="sortreverse"
    model.Pageno        ="pageno"
    model.Rowcount      ="rowcount"
    model.Totalno       ="totalno"
    model.Maxpageno     ="max_pageno"
    model.Fields        ="fields"
    model.Empties       ="empties"

    db.Exec(`DROP TABLE IF EXISTS testing`)
    db.Exec(`CREATE TABLE testing (id int auto_increment, x varchar(255), y varchar(255), primary key (id))`)

    args := make(map[string]interface{})
    model.SetDB(db)
    model.SetArgs(args)

    model.CurrentKey    = "id"
    model.CurrentIDAuto = "id"
    model.InsertPars    = []string{     "x","y"}
    model.TopicsPars    = []string{"id","x","y"}
    model.UpdatePars    = []string{"id","x","y"}
    model.EditPars      = []string{"id","x","y"}

    args["x"] = []string{"a"}
    args["y"] = []string{"b"}
    if err := model.Insert(); err != nil { panic(err) }
    log.Println(model.LastID)

    args["x"] = []string{"c"}
    args["y"] = []string{"d"}
    if err := model.Insert(); err != nil { panic(err) }
    log.Println(model.LastID)

    if err := model.Topics(); err != nil { panic(err) }
    log.Println(model.GetLists())

    args.Set("id","2")
    args["x"] = []string{"c"}
    args["y"] = []string{"z"}
    if err := model.Update(); err != nil { panic(err) }
    if err := model.Edit(); err != nil { panic(err) }
    log.Println(model.GetLists())

    os.Exit(0)
}
```

Running the program will result in

```bash
1
2
[map[id:1 x:a y:b] map[id:2 x:c y:d]]
[map[id:2 x:c y:z]]
```

</p>
</details>

<br /><br />


