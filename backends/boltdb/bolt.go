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


package boltdb

import "github.com/byte-mug/brute/utils"
import "github.com/byte-mug/brute/api"
import bolt "github.com/coreos/bbolt"
import "sync"

var kvPairs = []byte("kvpairs")

type Bolt struct{
	utils.MergeUtil
	DB      *bolt.DB
	
	wlock   sync.Mutex
}

func (b *Bolt) Submit(key, item []byte) (ok bool) {
	b.wlock.Lock(); defer b.wlock.Unlock()
	err := b.DB.Batch(func(txn *bolt.Tx) error{
		bkt,err := txn.CreateBucketIfNotExists(kvPairs)
		if err!=nil { return err }
		v := bkt.Get(key)
		if len(v)==0 {
			return bkt.Put(key,item)
		}
		r,ch := b.Merge(v,item)
		if ch {
			return bkt.Put(key,r)
		}
		return nil
	})
	return err==nil
}
func (b *Bolt) Obtain(key []byte) (item []byte,ok,readable bool) {
	b.DB.View(func(txn *bolt.Tx) error{
		bkt := txn.Bucket(kvPairs)
		if bkt==nil { return nil }
		item = bkt.Get(key)
		readable = true
		ok = len(item)>0
		return nil
	})
	return
}
func (b *Bolt) Stream(f func(key, item []byte)) {
	b.DB.View(func(txn *bolt.Tx) error{
		bkt := txn.Bucket(kvPairs)
		if bkt==nil { return nil }
		c := bkt.Cursor()
		for key,item := c.First(); len(key)!=0; key,item = c.Next() {
			f(key,item)
		}
		return nil
	})
}

func (b *Bolt) StartBatch() api.StorageFacade {
	tx,err := b.DB.Begin(true)
	if err!=nil { return b }
	return &BoltBatch{b,tx}
}

var _ api.StorageFacade = (*Bolt)(nil)

type BoltBatch struct{
	*Bolt
	Tx      *bolt.Tx
}

func (b *BoltBatch) Submit(key, item []byte) (ok bool) {
	item = append(make([]byte,0,len(item)),item...)
	bkt,err := b.Tx.CreateBucketIfNotExists(kvPairs)
	if err!=nil { return }
	ch := true
	
	if oitem := bkt.Get(key); len(oitem)>0 {
		item,ch = b.Merge(oitem,item)
	}
	
	if !ch { return true }
	ok = bkt.Put(key,item)!=nil
	return
}
func (b *BoltBatch) FinishBatch() error {
	return b.Tx.Commit()
}

var _ api.StorageFacade = (*BoltBatch)(nil)


