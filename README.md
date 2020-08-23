# taodbi
An abstract interface class to access the big data system TDengine in GO. Check *godoc* from [here](https://godoc.org/github.com/genelet/taodbi)

[![GoDoc](https://godoc.org/github.com/genelet/taodbi?status.svg)](https://godoc.org/github.com/genelet/taodbi)

[TDengine](https://github.com/taosdata/TDengine) is a very fast open-source database system. It comes with a [GO connector](https://github.com/taosdata/driver-go). This _taodbi_ GO package provides _abstract_ classes to access it, which some users may feel more convenient to use. In an advanced usage, one can call multiple *JOINed* tables in one statement, just like relational database.



### Installation

```
$ go get -u github.com/genelet/taodbi
```

Or manually clone from [github](https://github.com/genelet/taodbi) and place it under your GOPATH
```
$ git clone https://github.com/genelet/taodbi.git
```

There are three levels of usages: Basic, Map and Advanced.




## Chapter 1. BASIC USAGE


### 1.1) Data Type _DBI_

The struct _DBI_ is a wrapper of the standard _database/sql_ handle.
```
package taodbi

type DBI struct {
    Db        *sql.DB
    Affected  int64
}

```
where _Db_ is the database handle; _Affected_ saves affected number of rows after an operation.

#### Create a new handle

Use this function:
```
func Open(dataSourceName string) (*DB, error)
```

So you get a _DBI_ instance by:
```
&DBI{Db: created_handle}
```

#### Example

Create an instance; use it to create a new database and a table; add a row; and query the row:
```
package main

import (
    "log"
    "os"
    "github.com/genelet/taodbi"
)

func main() {
    db, err := taodbi.Open("root:taosdata@/tcp(127.0.0.1:0)/");
    if err != nil { panic(err) }
    defer db.Close()

    dbi := &taodbi.DBI{Db: db}

    err = dbi.ExecSQL(`CREATE DATABASE IF NOT EXISTS mydbi precision "us"`)
    if err != nil { panic(err) }
    err = dbi.ExecSQL(`USE mydbi`)
    if err != nil { panic(err) }
    err = dbi.ExecSQL(`DROP TABLE IF EXISTS mytable`)
    if err != nil { panic(err) }
    err = dbi.ExecSQL(`CREATE TABLE mytable 
(ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)`)
    if err != nil { panic(err) }
    err = dbi.ExecSQL(`INSERT INTO mytable (ts, id, name, len, flag, notes, fv, dv)
VALUES (now, ?, ?, 30, true, 'abcdefgh', 789.123, 456.789)`, 1234, `company`)
    if err != nil { panic(err) }
    lists := make([]map[string]interface{},0)
    err = dbi.QuerySQL(&lists,
`SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
    if err != nil { panic(err) }
    
    log.Printf("%v", lists)

    err = dbi.ExecSQL(`DROP DATABASE IF EXISTS mydbi`)
    if err != nil { panic(err) }

    os.Exit(0)
}
```
Running this example will report something like
```
[map[flag:true fv:789.123 id:1234 len:30 name:company ts:2020-07-19 09:07:48.341270]]
```


### 1.2) Execute an action on database or table, _ExecSQL_

```
err = dbi.ExecSQL(`CREATE DATABASE mytest`)
err = dbi.ExecSQL(`CREATE TABLE mytable 
        (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)`)
err = dbi.ExecSQL(`INSERT INTO mytable (ts, id, name, len, name, flag, notes, fv, dv)
        VALUES (now, ?, ?, 30, true, 'abcdefgh', 789.123, 456.789)`, 1234, `company`)
// after INSERT, dbi.Affected will be 1
```

Note that by default, any database function in this package will return *error* for errors, or *nil* for success. 


### 1.3) Execute using _DoSQL_ 

It does the same thing, but with a prepared statement and thus being safe for concurrent use by multiple goroutines.


### 1.4) Select using *QuerySQL*, *QuerySQLType* and *QuerySQLTypeLabel*

```
lists := make([]map[string]interface{})
err = dbi.QuerySQL(&lists,
        `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```
This will select all rows with _id=1234_ and put the result into *lists*, as an array of maps:
```
[
        {"ts":"2019-12-15 01:01:01.1234", "id":1234, "name":"company", "len":30, "flag":true, "fv":789.123},
        ....
]
```
The generic _database/sql_ will assign correct data types on most named variables, but may fail in few cases. e.g. a _tinyint_ may be assigned to be _int_. 

To get *EXACTLY* the needed data types, use *QuerySQLType*:
```
err = dbi.QuerySQLType(&lists, []string{"string", "int", "string", "int8", "bool", "float32},
        `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```
To change the columns names, use *QuerySQLTypeLabel*. E.g.
```
err = dbi.QuerySQLTypeLabel(&lists, []string{"string", "int", "string", "int8", "bool", "float32},
        []string{"lable_ts", "label_id", "label_name", "other_name1", "other2", "last3"},
        `SELECT ts, id, name, len, flag, fv FROM mytable WHERE id=?`, 1234)
```


### 1.5) Select using *SelectSQL*, *SelectSQLType* and *SelectSQlTypeLabel* 

They are similar to *QuerySQL*, *QuerySQLType* and *QuerySQLTypeLabel*, respectively, based on *prepared* statement.


### 1.6) Function *Quote*

This static function escape a string for unsafe characters *[';]*. You don't need to call it in the above *ExecSQL* and *QuerySQL* because we already do it.



## Chapter 2. MAP USAGE


Sometimes it is more flexible to insert data or search records by using *map* (i.e. *hash* or *associated array*). 
```
type Crud struct {
        DBI          
        CurrentTable string                 `json:"current_table"`
        CurrentKey   string                 `json:"current_key"`
        LastID       int64                  
        CurrentRow   map[string]interface{}
        Updated      bool
}

```
where *CurrentTable* is the table you are working with, *CurrentKey* the primary key in the table, always a [*timestamp*](https://www.taosdata.com/en/documentation/taos-sql/#Data-Query). *LastID*, *CurrentRow* and *Updated* are for the last inserted row.

You create an instance of *Crud* by
```
crud := &taodbi.Crud{Db:db, CurrentTable:mytable, CurrentKey:ts}
```


### 2.1) Insert one row, *InsertHash*
```
err = crud.InsertHash(map[string]interface{}{
        {"ts":"2019-12-31 23:59:59.9999", "id":7890, "name":"last day", "fv":123.456}
})
```
If you miss the primary key, the package will automatically assign *now* to be the value.


### 2.2) Insert or Retrieve an old row, *InsupdHash*

Sometimes a record may already be existing in the table, so you'd like to insert if it is not there, or retrieve it. Function *InsupdHash* is for this purpose:
```
err = crud.InsupdHash(map[string]interface{}{
        {"ts":"2019-12-31 23:59:59.9999", "id":7890, "name":"last day", "fv":123.456}},
        []string{"id","name"},
)
```
It identifies the uniqueness by the combined valuse of *id* and *name*. In both the cases, you get the ID in *crud.LastID*, the row in *CurrentRow*, and the case in *Updated* (true for old record, and false for new). 


### 2.3) Select many rows, *TopicsHash*

Search many by *TopicsHash*:
```
lists := make([]map[string]interface{})
restriction := map[string]interface{}{"len":10}
err = crud.TopicsHash(&lists, []string{"ts", "name", "id"}, restriction)
```
which returns all records with restriction *len=10*. You specifically define which columns to return in second argument,
which are *ts*, *name* and *id* here. 

Only three types of _restriction_ are supported in map:
- _key:value_  The *key* has *value*.
- _key:slice_  The *key* has one of values in *slice*.
- _"_gsql":"row sql statement"_  Use the special key *_gsql* to write a raw SQL statment.


### 2.4) Select one row, *EditHash*

```
lists := make([]map[string]interface{})
err = crud.EditHash(&lists, []string{"ts", "name", "id"}, "2019-12-31 23:59:59.9999")
```
Here you select by its primary key value (the timestamp). 

Optionally, you may input an array of few key values and get them all in *lists*. Or you may put a restriction map too.




## Chapter 3. ADVANCED USAGE

*Model* is even a more detailed class operation on TDengine table.

### 3.1) Class *Model*

```
type Model struct {
    Crud `json:"crud,omitempty"`

    ARGS  map[string]interface{}   `json:"args,omitempty"`
    LISTS []map[string]interface{} `json:"lists,omitempty"`
    OTHER map[string]interface{}   `json:"other,omitempty"`

    SORTBY      string `json:"sortby,omitempty"`
    SORTREVERSE string `json:"sortreverse,omitempty"`
    PAGENO      string `json:"pageno,omitempty"`
    ROWCOUNT    string `json:"rowcount,omitempty"`
    TOTALNO     string `json:"totalno,omitempty"`

    Nextpages map[string][]map[string]interface{} `json:"nextpages,omitempty"`
    Storage   map[string]map[string]interface{}   `json:"storage,omitempty"`

    InsertPars []string `json:"insert_pars,omitempty"`
    InsupdPars []string `json:"insupd_Pars,omitempty"`

    EditPars   []string          `json:"edit_pars,omitempty"`
    TopicsPars []string          `json:"topics_pars,omitempty"`
    EditMap    map[string]string `json:"edit_map,omitempty"`
    TopicsMap  map[string]string `json:"topics_map,omitempty"`

    TotalForce int `json:"total_force,omitempty"`
}

```

####  3.1.1) Table column names
- _InsertPars_ defines column names used for insert a new data
- _InsupdPars_ defines column names used for uniqueness 
- _EditPars_ defines which columns to be returned in *search one* action *Edit*.
- _TopicsPars_ defines which columns to be returned in *search many* action *Topics*.

#### 3.1.2) Incoming data *ARGS*

- Case *search many*, it contains data for *pagination*. 
- Case *insert*, it stores the new row as hash (so the package takes column values of _EditPars_ from *ARGS*).

#### 3.1.3) Output *LISTS*

In case of *search* (*Edit* and *Topics*), the output data are stored in *LISTS*.

#### 3.1.4) Pagination, used in *Topics*.
- *ARGS[SORTBY]* defines sorting by which column
- *ARGS[SORTREVERSE]* defines if a reverse sort
- *ARGS[ROWCOUNT]* defines how many records on each page, an incoming data
- *ARGS[PAGENO]* defines which page number, an incoming data
- *ARGS[TOTALNO]* defines total records available, an output data

Based on those information, developers can build paginations.

#### 3.1.5) Nextpages, calling multiple tables

In many applications, your data involve multiple tables. This function is especially important in TDengine because it's not a relational database and thus has no *JOIN* to use. 

You can define the retrival logic in *Nextpages*, usually expressed as a JSON struct. Assuming there are three *model*s: *testing1*, *testing2* and *testing3*, and you are working in *testing1* now. 
```
"nextpages": {
    "topics" : [
      {"model":"testing2", "action": "topics", "relate_item":{"id":"fid"}},
      {"model":"testing3", "action": "topics"}
    ] ,
    "edit" : [...]
}
```

Thus when you run "topics" on the current model *testing1*, another action "topics" on model "testing2" will be triggered for each returned row. The new action on *testing2* is restricted to have that its column *fid* the same value as *testing1*'s *id*, as in *relate_item*. 

The returned data will be attached to original row under the special key named *testing2_topics*.

- Meanwhile, the above example runs action *topics* on *testing2* once, because there is no *relate_item* in the record.
- The returned will be stored in class variable *OTHER* under key *testing3_topics*.


### 3.2) Create an instance, *NewModel*

An instance is usually created from a JSON string defining the table schema and logic in relationship between tables:
```
model, err := &taodbi.Model(json_string)
if err != nil {
        panic(err)
}
// create dbi as above
model.DBI = dbi
// create args as a map
model.ARGS = args

```

If you need to call mutiple tables in one function, you need to put other model instances into *Storage*:
```
// create the database handler "db"
    c := newconf("config.json")
    db, err := sql.Open(c.DbType, c.Dsn2)
    if err != nil { panic(err) }
    
// create your current model named "atesting", note that we have nextpages in it 
    model, err := NewModel(`{
    "crud": {
        "current_table": "atesting",
        "current_key" : "id"
        },
    "insupd_pars" : ["x","y"],
    "insert_pars" : ["x","y","z"],
    "edit_pars" : ["x","y","z","id"],
    "topics_pars" : ["id","x","y","z"],
    "nextpages" : {
        "topics" : [
            {"model":"testing", "action":"topics", "relate_item":{"id":"id"}}
        ]
    }
}`)
    if err != nil { panic(err) }
    model.Db = db
    model.ARGS  = make(map[string]interface{})
    model.OTHER = make(map[string]interface{})

// create another model with name "testing"
    st, err := NewModel(`{
    "crud": {
        "current_table": "testing",
        "current_key" : "tid"
    },
    "insert_pars" : ["id","child"],
    "edit_pars"   : ["tid","child","id"],
    "topics_pars" : ["tid","child","id"]
}`)
    if err != nil { panic(err) }
    st.Db = db
    st.ARGS  = make(map[string]interface{})
    st.OTHER = make(map[string]interface{})

// create a storage to mark "testing"
    storage := make(map[string]map[string]interface{})
    storage["model"]= make(map[string]interface{})
    storage["model"]["testing"]= st
    storage["action"]= make(map[string]interface{})
    tt := make(map[string]interface{})
    tt["topics"] = func(args ...map[string]interface{}) error {
        return st.Topics(args...)
    }
    storage["action"]["testing"] = tt
```

### 3.3) Actions (functions) on *Model*

#### 3.3.1) Insert one row, *Insert*
```
err = model.Insert()
```
It will takes values from *ARGS* using pre-defined column names in *InsertPars*. If you miss the primary key, the package will automatically assign *now* to be the value.


#### 3.3.2) Insert or Retrieve an old row, *Insupd*

You insert one row as in *Insert* but if it is already in the table, retrieve it.
```
err = model.Insupd()
```
It identifies the uniqueness by the combined column valuse defined in *InsupdPars*. In both the cases, you get the ID in *model.LastID*, the row in *CurrentRow*, and the case in *Updated* (true for old record, and false for new). 


#### 3.3.3) Select many rows, *Topics*

Search many by *Topics*:
```
restriction := map[string]interface{}{"len":10}
err = crud.Topics(restriction)
```
which returns all records with columns defined in *TopicsPars* with restriction *len=10*. The returned data is in *model.LISTS*

Since you have assigned *nextpages* for this action, for each row it retrieve, another *Topics* on model *testing* will run using the constraint that *id* in *testing* should take the value in the original row.


#### 3.3.4) Select one row, *Edit*

```
err = crud.EditHash()
```
Here you select by its primary key value (the timestamp), which is assumed to be in *ARGS*. The returned data is in *model.LISTS*. Optionally, you may put a restriction. 


#### 3.3.5) Sort order, *OrderString*

This returns you the sort string used in the *select many*. If you inherit this class you can override this function to use your own sorting logic.


## SAMPLES

Please check those test files:

- DBI: [dbi_test.go](https://github.com/genelet/taodbi/blob/master/dbi_test.go)
- Crud: [crud_test.go](https://github.com/genelet/taodbi/blob/master/crud_test.go)
- Model: [model_test.go](https://github.com/genelet/taodbi/blob/master/model_test.go)









