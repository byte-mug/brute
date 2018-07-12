/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package sqllog

import (
	"github.com/byte-mug/brute/replicator"
	"database/sql"
	"time"
	"os"
)

type DBType uint
const (
	DBT_QL DBType = iota
	DBT_PQ
)
func (t DBType) sqlArg(s string) string {
	switch t {
	case DBT_QL,DBT_PQ:
		return "$"+s
	}
	return "?"
}
func (t DBType) sqlType(s string) string {
	switch t {
	case DBT_QL:
		return s
	case DBT_PQ:
		switch s {
		case "string": return "text"
		case "time": return "timestamp"
		case "blob": return "bytea"
		}
		return s
	}
	return s
}
func (t DBType) subst(s string) string {
	if len(s)>=2 {
		switch s[:2]{
		case "a-":
			return t.sqlArg(s[2:])
		case "t-":
			return t.sqlType(s[2:])
		}
	}
	if len(s)>=1 {
		switch s[0] {
		case 'a':
			return t.sqlArg(s[1:])
		}
	}
	switch s {
	case "now":
		return "now()"
	case "unique":
		return "UNIQUE"
	}
	return ""
}
func (t DBType) Expand(s string) string {
	return os.Expand(s,t.subst)
}

type TypedDB struct {
	*sql.DB
	DBType
}
func (t *TypedDB) Mutate(query string, args ...interface{}) (sql.Result, error) {
	switch t.DBType {
	case DBT_QL:
		tx,err := t.Begin()
		if err!=nil { return nil,err }
		r,err := tx.Exec(query,args...)
		if err!=nil { tx.Rollback() } else { err = tx.Commit() }
		return r,err
	}
	return t.Exec(query,args...)
}

const DbTimeVecTable = `
	CREATE TABLE tmvec (
		v_node ${t-string} NOT NULL,
		v_time ${t-time} NOT NULL );
	CREATE ${unique} INDEX tmvec_pk ON tmvec (v_node);
`


type DbTimeVec struct {
	DB TypedDB
}
func (db *DbTimeVec) Init() {
	db.DB.Exec(DbTimeVecTable)
}
func (db *DbTimeVec) Query(t *replicator.TimeVecQuery) error {
	err := db.DB.QueryRow(db.DB.Expand(`SELECT v_time FROM tmvec WHERE v_node = $a1`),t.Node).Scan(&t.Value)
	t.Exist = err==nil
	if err==sql.ErrNoRows {
		return nil
	}
	return err
}
func (db *DbTimeVec) Update(t *replicator.TimeVecQuery) (err error) {
	if t.Exist {
		_,err = db.DB.Mutate(db.DB.Expand(`UPDATE tmvec SET v_time = $a1 WHERE v_node = $a2`),t.Value,t.Node)
	} else {
		_,err = db.DB.Mutate(db.DB.Expand(`INSERT INTO tmvec (v_node,v_time) VALUES ($a1,$a2)`),t.Node,t.Value)
	}
	return
}
func (db *DbTimeVec) Extract(r map[string]time.Time) error {
	rs,err := db.DB.Query(`SELECT v_node,v_time FROM tmvec`)
	if err!=nil { return err }
	var k string
	var v time.Time
	for rs.Next() {
		err = rs.Scan(&k,&v)
		if err!=nil { return err }
		r[k] = v
	}
	return nil
}

const DbLocalUpdateLogTable = `
	CREATE TABLE updlog (
		u_key  ${t-blob} NOT NULL,
		u_time ${t-time} NOT NULL );
	CREATE ${unique} INDEX updlog_pk ON updlog (u_key);
	CREATE INDEX updlog_tm ON updlog (u_time);
`

type DbLocalUpdateLog struct {
	DB TypedDB
}
func (db *DbLocalUpdateLog) Query(t *replicator.LocalUpdateEntry) error {
	err := db.DB.QueryRow(db.DB.Expand(`SELECT u_time FROM updlog WHERE u_key = $a1`),t.Key).Scan(&t.Change)
	t.Exist = err==nil
	if err==sql.ErrNoRows {
		return nil
	}
	return err
}
func (db *DbLocalUpdateLog) Update(t *replicator.LocalUpdateEntry) (err error) {
	if t.Exist {
		_,err = db.DB.Mutate(db.DB.Expand(`UPDATE updlog SET u_time = $a1 WHERE u_key = $a2`),t.Change,t.Key)
	} else {
		_,err = db.DB.Mutate(db.DB.Expand(`INSERT INTO updlog (u_key,u_time) VALUES ($a1,$a2)`),t.Key,t.Change)
	}
	return
}

func (db *DbLocalUpdateLog) ReadAllAsync(since time.Time, targ chan <- replicator.LocalUpdateEntry) error {
	rs,err := db.DB.Query(db.DB.Expand(`SELECT u_key,u_time FROM updlog WHERE u_time > $a1`),since)
	if err!=nil { return err }
	go func() {
		var lue replicator.LocalUpdateEntry
		lue.Exist = true
		defer close(targ)
		for rs.Next() {
			err = rs.Scan(&lue.Key,&lue.Change)
			if err!=nil { continue }
			targ <- lue
		}
	}()
	return nil
}

