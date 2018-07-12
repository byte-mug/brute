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

import "github.com/julienschmidt/httprouter"
import "net/http"

import "github.com/byte-mug/brute/replicator"
import "github.com/byte-mug/brute/api"
import "encoding/base64"
import "github.com/vmihailenco/msgpack"
import "bytes"
import "bufio"

type Server struct {
	Node string
	DBN string
	Vec replicator.TimeVec
	Log replicator.LocalUpdateLog
	Api api.StorageFacade
}
func (s *Server) getSince(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	bts,err := base64.RawURLEncoding.DecodeString(ps.ByName("since"))
	if err!=nil {
		w.WriteHeader(400)
		return
	}
	ts,err := msgpack.NewDecoder(bytes.NewReader(bts)).DecodeTime()
	if err!=nil {
		w.WriteHeader(400)
		return
	}
	
	lues := make(chan replicator.LocalUpdateEntry,1024)
	err = s.Log.ReadAllAsync(ts,lues)
	if err!=nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
	w.Header().Add("Content-Type","application/x-msgpack")
	llw := bufio.NewWriter(w)
	enc := msgpack.NewEncoder(llw)
	defer llw.Flush()
	for lue := range lues {
		item,ok,readable := s.Api.Obtain(lue.Key)
		ok = ok&&readable
		if !ok { item = nil }
		err = enc.Encode(lue.Key,lue.Change,ok,item)
		if err!=nil { return }
	}
}
func (s *Server) getVersion(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Add("Content-Type","application/x-msgpack")
	llw := bufio.NewWriter(w)
	enc := msgpack.NewEncoder(llw)
	defer llw.Flush()
	
	tvq := &replicator.TimeVecQuery{Node: s.Node}
	s.Vec.Query(tvq)
	enc.Encode(tvq.Exist,tvq.Value)
}
func (s *Server) Register(r *httprouter.Router) {
	r.GET("/"+s.DBN+"/p2p-s/:since",s.getSince  )
	r.GET("/"+s.DBN+"/p2p-v"       ,s.getVersion)
}


