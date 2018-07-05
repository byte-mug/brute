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


package api

/* Brute is eventually consistent */


/*
In Brute, an Item is a write operation and a record at the same time (excluding
the Key). All write operations are required to be idempotent and commutative.

Brute is designed to store Key-Item-Pairs. Pairs with the same Key are merged,
requiring these merge operations to be idempotent and commutative.

(This type is only here to attach some documentation onto it!)
*/
type Item []byte

/*
The merger is a state automaton, that operates during reconciliation of
"conflicting" writes (eg. writes that have the same key).
*/
type Merger interface{
	/* Clears and Inits the Merger, supplying the first Item. */
	Init(item []byte)
	
	/* Merge another item */
	Merge(item []byte)
	
	/*
	Returns true, if the Item supplied to .Init() differs from the Item
	returned by .Result()!
	
	Returns false otherwise.
	*/
	Changed() bool
	
	/* Returns the Items resulting from the merge operation. */
	Result() []byte
	
	/* Cleans up the structure. Can be used to reduce the Memory-Footprint. */
	Cleanup()
}

/*
As a Merger-Object is single threaded, multiple instances are needed during
multi-threaded operations.

For this, the MergerFactory is supplied.
*/
type MergerFactory func() Merger

/*
An abstraction ontop of a Key-Item store.

A StorageFacade is assumed to posess a MergerFactory, so that this does not need to be supplied to it.
*/
type StorageFacade interface{
	/*
	Submits (and potentially merges) a key-item-pair.
	
	Returns false, if the store was not writable during that operation.
	*/
	Submit(key, item []byte) (ok bool)
	
	/*
	Obtains an Item for the supplied key.
	
	If ok==false, no item had been found.
	
	If readable==false, the store was not readable during that operation.
	*/
	Obtain(key []byte) (item []byte,ok,readable bool)
	
	Stream(f func(key, item []byte))
}


