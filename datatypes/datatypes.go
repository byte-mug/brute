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


package datatypes

import "github.com/byte-mug/golibs/msgpackx"
import "github.com/byte-mug/brute/api"
import "github.com/vmihailenco/msgpack"
import "bytes"
import "time"

type LastWriteWins struct{
	changed bool
	updated time.Time
	item []byte
}

func (lww *LastWriteWins) Init(item []byte) {
	lww.changed = false
	lww.item = item
	t,err := msgpack.NewDecoder(bytes.NewReader(item)).DecodeTime()
	if err!=nil {
		lww.updated = time.Time{}
	} else {
		lww.updated = t
	}
}

func (lww *LastWriteWins) Merge(item []byte) {
	t,err := msgpack.NewDecoder(bytes.NewReader(item)).DecodeTime()
	if err!=nil { return }
	if lww.updated.Before(t) {
		lww.updated = t
		lww.item = item
		lww.changed = true
	}
}

func (lww *LastWriteWins) Changed() bool {
	return lww.changed
}

func (lww *LastWriteWins) Result() []byte {
	return lww.item
}

func (lww *LastWriteWins) Cleanup() {
	lww.item = nil
}

var _ api.Merger = (*LastWriteWins)(nil)
func LWW_Factory() api.Merger { return new(LastWriteWins) }

func LWW_Put(value []byte) (item []byte) {
	t := time.Now().UTC()
	item,_ = msgpackx.Marshal(t,true,value)
	return
}
func LWW_Delete() (item []byte) {
	t := time.Now().UTC()
	item,_ = msgpackx.Marshal(t,false)
	return
}

func LWW_Decode(item []byte, in_ok,readable bool) (value []byte,ok bool) {
	ok = in_ok
	if !(ok&&readable) { return nil,false }
	dec := msgpack.NewDecoder(bytes.NewReader(item))
	_,err := dec.DecodeTime()
	if err!=nil { return nil,false }
	ok,err = dec.DecodeBool()
	if err!=nil { return nil,false }
	if !ok { return }
	value,err = dec.DecodeBytes()
	if err!=nil { return nil,false }
	return
}


