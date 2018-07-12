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


package updater

import "github.com/byte-mug/brute/replicator"
import "github.com/byte-mug/brute/api"

import "time"

import "github.com/dgryski/go-farm"
import "sync"

func tmMax(a, b time.Time) time.Time {
	if a.Before(b) { return b }
	return a
}

const nLocks = 64

type Updater struct {
	Local replicator.TimeVecQuery
	TmVec replicator.TimeVec
	UpLog replicator.LocalUpdateLog
	
	Store api.StorageFacade
	
	locks [nLocks]sync.Mutex
	lock  sync.Mutex
}

func (a *Updater) Init() error {
	return a.TmVec.Query(&a.Local)
}
func (a *Updater) time() time.Time {
	a.lock.Lock(); defer a.lock.Unlock()
	tm := tmMax(a.Local.Value.Add(1),time.Now().UTC())
	a.Local.Value = tm
	return tm
}
func (a *Updater) writeback() error {
	a.lock.Lock(); defer a.lock.Unlock()
	err := a.TmVec.Update(&a.Local)
	if err!=nil { return err }
	a.Local.Exist = true
	return nil
}
func (a *Updater) updat(tm time.Time,key []byte) error {
	lock := &a.locks[farm.Hash32(key)%nLocks]
	lock.Lock(); defer lock.Unlock()
	
	lue := &replicator.LocalUpdateEntry{Key:key}
	err := a.UpLog.Query(lue)
	if err!=nil { return err }
	lue.Change = tm
	err = a.UpLog.Update(lue)
	return err
}
func (a *Updater) bump(key []byte) (ok bool) {
	tm := a.time()
	
	err := a.updat(tm,key)
	if err!=nil { return false }
	
	err = a.writeback()
	if err!=nil { return false }
	
	return true
}
func (a *Updater) Submit(key, item []byte) (ok bool) {
	ok = a.bump(key)
	if !ok { return }
	ok = a.Store.Submit(key,item)
	return
}
func (a *Updater) Obtain(key []byte) (item []byte,ok,readable bool) {
	return a.Store.Obtain(key)
}
func (a *Updater) Stream(f func(key, item []byte)) {
	a.Store.Stream(f)
}

