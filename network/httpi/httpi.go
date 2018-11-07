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

import "github.com/byte-mug/brute/api"
import "encoding/base64"
import "github.com/vmihailenco/msgpack"
import "bufio"
import "bytes"
import "io/ioutil"

type Server struct {
	DBN string
	Api api.StorageFacade
}
func (s *Server) obtgain(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key,err := base64.RawURLEncoding.DecodeString(ps.ByName("key"))
	if err!=nil {
		w.WriteHeader(400)
		return
	}
	item,ok,readable := s.Api.Obtain(key)
	if !readable {
		w.WriteHeader(500)
	} else if !ok {
		w.WriteHeader(404)
	} else {
		w.Header().Add("Content-Type","application/octet-stream")
		w.WriteHeader(200)
		w.Write(item)
	}
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key,err := base64.RawURLEncoding.DecodeString(ps.ByName("key"))
	if err!=nil {
		w.WriteHeader(400)
		return
	}
	item,err := ioutil.ReadAll(r.Body)
	if err!=nil {
		w.WriteHeader(400)
		return
	}
	ok := s.Api.Submit(key,item)
	if !ok {
		w.WriteHeader(500)
	} else {
		w.WriteHeader(202)
	}
}
func (s *Server) stream(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Add("Content-Type","application/x-msgpack")
	llw := bufio.NewWriter(w)
	enc := msgpack.NewEncoder(llw)
	defer llw.Flush()
	s.Api.Stream(func(key, item []byte){
		enc.EncodeBytes(key)
		enc.EncodeBytes(item)
	})
}
func (s *Server) Register(r *httprouter.Router) {
	u1 := "/"+s.DBN+"/api-r/:key"
	r.Handle("GET",u1,s.obtgain)
	r.Handle("PUT",u1,s.submit)
	r.Handle("POST",u1,s.submit)
	r.GET("/"+s.DBN+"/api-stream",s.stream)
}

type Client struct{
	Addr string
	DBN string
	Shared *http.Client
}
func (c *Client) url(key []byte) string {
	return "http://"+c.Addr+"/"+c.DBN+"/api-r/"+base64.RawURLEncoding.EncodeToString(key)
}
func (c *Client) Submit(key, item []byte) (ok bool) {
	resp,err := c.Shared.Post(c.url(key),"application/octet-stream",bytes.NewReader(item))
	if err!=nil { return }
	resp.Body.Close()
	ok = resp.StatusCode==202
	return
}
func (c *Client) Obtain(key []byte) (item []byte,ok,readable bool) {
	resp,err := c.Shared.Get(c.url(key))
	if err!=nil { return }
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		item,_ = ioutil.ReadAll(resp.Body)
		ok = true
		fallthrough
	case 404:
		readable = true
	}
	return
}
func (c *Client) Stream(f func(key, item []byte)) {
	req,err := http.NewRequest("GET","http://"+c.Addr+"/"+c.DBN+"/api-stream/",nil)
	if err!=nil { return }
	resp,err := c.Shared.Do(req)
	if err!=nil { return }
	defer resp.Body.Close()
	dec := msgpack.NewDecoder(bufio.NewReader(resp.Body))
	var key,item []byte
	i := []interface{}{&key,&item}
	for {
		err = dec.DecodeMulti(i...)
		if err!=nil { return }
		f(key,item)
	}
}

var _ api.StorageFacade = (*Client)(nil)
