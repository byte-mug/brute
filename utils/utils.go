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


package utils

import "github.com/byte-mug/brute/api"
import "sync"

type MergeUtil struct{
	Merger  api.MergerFactory
	pool    sync.Pool
}
func (b *MergeUtil) getMerger() api.Merger {
	i := b.pool.Get()
	if i==nil { return b.Merger() }
	return i.(api.Merger)
}
func (b *MergeUtil) Merge(items ...[]byte) ([]byte,bool) {
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

