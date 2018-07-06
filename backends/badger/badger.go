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


package bakbadger

import "github.com/byte-mug/brute/api"
import "github.com/dgraph-io/badger"
import "sync"

type Badger struct{
	DB      *badger.DB
	Merger  api.MergerFactory
	
	pool    sync.Pool
	wlock   sync.Mutex
}
func (b *Badger) getMerger() api.Merger {
	i := b.pool.Get()
	if i==nil { return b.Merger() }
	return i.(api.Merger)
}
func (b *Badger) merge(items ...[]byte) ([]byte,bool) {
	m := b.getMerger()
	defer b.pool.Put(m)
	for i,item := range items {
		if i==0 {
			m.Init(item)
		} else {
			m.Merge(item)
		}
	}
	ch := m.Changed()
	ret := m.Result()
	m.Cleanup()
	return ret,ch
}
func (b *Badger) Submit(key, item []byte) (ok bool) {
	b.wlock.Lock(); defer b.wlock.Unlock()
	err := b.DB.Update(func(txn *badger.Txn) error{
		w,err := txn.Get(key)
		if err==badger.ErrKeyNotFound {
			return txn.Set(key,item)
		}
		if err!=nil { return nil }
		v,err := w.Value()
		if err!=nil { return err }
		r,ch := b.merge(v,item)
		if ch {
			return txn.Set(key,r)
		}
		return nil
	})
	return err==nil
}
func (b *Badger) Obtain(key []byte) (item []byte,ok,readable bool) {
	b.DB.View(func(txn *badger.Txn) error{
		elem,err := txn.Get(key)
		if err==badger.ErrKeyNotFound { readable = true }
		if err!=nil { return nil }
		item,err = elem.Value()
		ok = err==nil
		readable = true
		return nil
	})
	return
}
func (b *Badger) Stream(f func(key, item []byte)) {
	b.DB.View(func(txn *badger.Txn) error{
		iter := txn.NewIterator(badger.IteratorOptions{PrefetchValues:true,PrefetchSize:128})
		defer iter.Close()
		for iter.Next(); iter.Valid() ;iter.Next() {
			i := iter.Item()
			v,err := i.Value()
			if err!=nil { return err }
			f(i.Key(),v)
		}
		return nil
	})
}

var _ api.StorageFacade = (*Badger)(nil)
