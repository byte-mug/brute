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

import "time"
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/brute/api"

type tableRowField struct{
	ts time.Time
	value interface{}
}
func (t *tableRowField) DecodeMsgpack(dec *msgpack.Decoder) (err error) {
	t.ts,err = dec.DecodeTime()
	if err!=nil { return }
	t.value,err = dec.DecodeInterface()
	return
}
func (t *tableRowField) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(t.ts,t.value)
}

type tableRow map[string]*tableRowField

type TableMerger struct {
	changed bool
	currentRow tableRow
	dts time.Time
}
func (t *TableMerger) Init(item []byte) {
	t.changed = false
	t.currentRow = nil
	t.dts = time.Time{}
	var m tableRow
	var dts time.Time
	if msgpack.Unmarshal(item,&dts,&m)!=nil { return }
	t.currentRow = m
	t.dts = dts
}
func (t *TableMerger) Merge(item []byte) {
	var m tableRow
	var dts time.Time
	if msgpack.Unmarshal(item,&dts,&m)!=nil { return }
	if t.currentRow==nil {
		t.currentRow = m
		t.changed = true
	} else {
		if t.dts.Before(dts) { t.dts = dts } else { dts = t.dts }
		for k,v := range m {
			if v.ts.Before(dts) { continue }
			ov,ok := t.currentRow[k]
			if !ok {
				t.currentRow[k] = v
				t.changed = true
			} else if ov.ts.Before(v.ts) {
				t.currentRow[k] = v
				t.changed = true
			}
		}
		var lst []string
		for k := range t.currentRow {
			if t.currentRow[k].ts.Before(dts) {
				lst = append(lst,k)
			}
		}
		for _,k := range lst {
			delete(t.currentRow,k)
			t.changed = true
		}
	}
}
func (t *TableMerger) Changed() bool {
	return t.changed
}
func (t *TableMerger) Result() []byte {
	bts,_ := msgpack.Marshal(t.dts,t.currentRow)
	return bts
}
func (t *TableMerger) Cleanup() { t.currentRow = nil }

var _ api.Merger = (*TableMerger)(nil)
func Table_Factory() api.Merger { return new(TableMerger) }

type Row map[string]interface{}

func Table_Put(src Row) (item []byte) {
	t := time.Now().UTC()
	m := make(tableRow)
	for k,v := range src { m[k] = &tableRowField{t,v} }
	
	item,_ = msgpack.Marshal(time.Time{},m)
	return
}
func Table_Delete() (item []byte) {
	t := time.Now().UTC()
	m := make(tableRow)
	item,_ = msgpack.Marshal(t,m)
	return
}

func Table_Decode(item []byte, in_ok,readable bool) (value Row,ok bool) {
	ok = in_ok
	if !(ok&&readable) { return nil,false }
	var m tableRow
	var dts time.Time
	err := msgpack.Unmarshal(item,&dts,&m)
	if err!=nil { return nil,false }
	if len(m)==0 { return nil,false }
	value = make(Row)
	for k,v := range m {
		value[k] = v.value
	}
	return
}

