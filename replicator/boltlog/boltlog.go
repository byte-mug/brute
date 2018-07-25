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


package boltlog

import bolt "github.com/coreos/bbolt"
import "github.com/byte-mug/brute/replicator"
import "github.com/vmihailenco/msgpack"
import "time"

const TSF = "20060102150405.000000000"
/*
There are two tables:
- Table
- Index

we have the following pair type: l.Key => l.Change

Table[l.Key] => l.Change
Index[(l.Key,l.Change)=>] => l.Key
*/

func makeKey2(buf []byte,t *time.Time,key []byte) []byte {
	buf = t.UTC().AppendFormat(buf[:0],TSF)
	buf = append(buf,key...)
	return buf
}

type BoltLocalUpdateLog struct{
	DB *bolt.DB
	Table, Index []byte
}
func (q *BoltLocalUpdateLog) Query(l *replicator.LocalUpdateEntry) error {
	return q.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(q.Table)
		l.Exist = false
		if bkt==nil { return nil }
		if msgpack.Unmarshal(bkt.Get(l.Key),&l.Change)==nil {
			l.Exist = true
		}
		return nil
	})
}
func (q *BoltLocalUpdateLog) Update(l *replicator.LocalUpdateEntry) error {
	err := q.DB.Batch(func(tx *bolt.Tx) error {
		bkt,err := tx.CreateBucketIfNotExists(q.Table)
		if err!=nil { return err }
		idx,err := tx.CreateBucketIfNotExists(q.Index)
		if err!=nil { return err }
		var t time.Time
		var ibuf []byte
		if msgpack.Unmarshal(bkt.Get(l.Key),&t)==nil {
			ibuf = makeKey2(ibuf,&t,l.Key)
			idx.Delete(ibuf)
		}
		data,err := msgpack.Marshal(l.Change)
		if err!=nil { return err }
		if msgpack.Unmarshal(data,&t)==nil {
			ibuf = makeKey2(ibuf,&t,l.Key)
			err = idx.Put(ibuf,l.Key)
			if err!=nil { return err }
		}
		return bkt.Put(l.Key,data)
	})
	if err==nil {
		l.Exist = true
	}
	return err
}

/* This function MUST return immediately and feed 'targ' in background. */
func (q *BoltLocalUpdateLog) ReadAllAsync(since time.Time, targ chan <- replicator.LocalUpdateEntry) error {
	chr := make(chan error,1)
	tmb := since.UTC().AppendFormat(nil,TSF)
	go q.DB.View(func(tx *bolt.Tx) (e2 error) {
		defer close(chr)
		idx := tx.Bucket(q.Index)
		if idx==nil { return nil } // Nothing to return!
		bkt := tx.Bucket(q.Table)
		cur := idx.Cursor()
		chr <- nil
		defer close(targ)
		for k,v := cur.Seek(tmb); len(k)!=0; k,v = cur.Next() {
			nv := make([]byte,len(v))
			copy(nv,v)
			t,err := time.Parse(TSF,string(k[:len(TSF)]))
			if err!=nil {
				if bkt==nil { continue }
				if msgpack.Unmarshal(bkt.Get(v),&t)!=nil { continue }
			}
			targ <- replicator.LocalUpdateEntry{ Key: v, Change: t, Exist: true}
		}
		return nil
	})
	return <-chr
}
