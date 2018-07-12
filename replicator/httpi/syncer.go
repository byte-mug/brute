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


package httpi

import "net/http"

import "github.com/byte-mug/brute/replicator"
import "github.com/byte-mug/brute/api"
import "encoding/base64"
import "github.com/vmihailenco/msgpack"
import "bufio"
import "time"

func tmMax(a, b time.Time) time.Time {
	if a.Before(b) { return b }
	return a
}

type StartBatch interface{
	StartBatch() api.StorageFacade
}
type FinishBatch interface{
	FinishBatch() error
}

func startBatch(s api.StorageFacade) api.StorageFacade {
	if sb,ok := s.(StartBatch) ; ok { return sb.StartBatch() }
	return s
}
func finishBatch(s api.StorageFacade) error {
	if fb,ok := s.(FinishBatch) ; ok { return fb.FinishBatch() }
	return nil
}

type Syncer struct {
	DBN string
	Shared *http.Client
	Vec replicator.TimeVec
	Api api.StorageFacade
}
func (s *Syncer) GetVersion(node,addr string) (*time.Time,error) {
	var t time.Time
	var exist bool
	ue := "http://"+addr+"/"+s.DBN+"/p2p-v"
	resp,err := s.Shared.Get(ue)
	if err!=nil { return nil,err }
	defer resp.Body.Close()
	dec := msgpack.NewDecoder(bufio.NewReader(resp.Body))
	
	err = dec.Decode(&exist,&t)
	if err!=nil { return nil,err }
	
	if exist { return &t,nil }
	return nil,nil
}
func (s *Syncer) SyncWith(node,addr string, remote *time.Time) error {
	tvq := &replicator.TimeVecQuery{Node:node}
	err := s.Vec.Query(tvq)
	if err!=nil { return err }
	
	if remote!=nil {
		if !remote.After(tvq.Value) { return nil } /* Replica is not stale! */
	}
	
	btm,err := msgpack.Marshal(tvq.Value)
	if err!=nil { return err }
	ue := "http://"+addr+"/"+s.DBN+"/p2p-s/"+base64.RawURLEncoding.EncodeToString(btm)
	resp,err := s.Shared.Get(ue)
	if err!=nil { return err }
	defer resp.Body.Close()
	dec := msgpack.NewDecoder(bufio.NewReader(resp.Body))
	
	var key,item []byte
	var change time.Time
	var ok bool
	i := []interface{}{&key,&change,&ok,&item}
	
	batch := startBatch(s.Api)
	//batch := (s.Api)
	
	for {
		err = dec.Decode(i...)
		if err!=nil { break }
		if ok {
			ok = batch.Submit(key,item)
		} else {
			ok = true
		}
		if ok { tvq.Value = tmMax(tvq.Value,change) }
	}
	
	err = finishBatch(batch)
	if err!=nil { return err }
	return s.Vec.Update(tvq)
}
