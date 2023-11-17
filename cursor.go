package tempdb

import (
	"bytes"
	"errors"
	"sort"
)

type pair struct {
	key   []byte
	value []byte
}

type Cursor struct {
	bucket *Bucket
	keys   []string
	index  int
}

func (c *Cursor) get(index int) ([]byte, []byte) {
	if index >= len(c.keys) {
		return nil, nil
	}

	k := c.keys[index]
	v := c.bucket.value[k]

	return []byte(k), v
}

func (c *Cursor) First() ([]byte, []byte) {
	return c.get(0)
}

func (c *Cursor) Last() ([]byte, []byte) {
	return c.get(len(c.keys) - 1)
}

func (c *Cursor) Next() ([]byte, []byte) {
	c.index++
	return c.get(c.index)
}

func (c *Cursor) Prev() ([]byte, []byte) {
	return c.get(c.index - 1)
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	var k []byte
	var v []byte

	b := c.index
	f := false

	// iterate from the current index
	for k, v = c.get(c.index); k != nil; k, v = c.Next() {
		if bytes.Equal(k, seek) {
			f = true
			break
		}
	}

	// return the next key/value if we can't find the key.

	if !f {
		c.index = b

		return c.Next()
	}

	return k, v
}

func (c *Cursor) Delete() error {
	k, _ := c.get(c.index)
	if k == nil {
		return errors.New("current index out of range")
	}

	return c.bucket.Delete(k)
}

func newCursor(bkt *Bucket) *Cursor {
	c := &Cursor{
		bucket: bkt,
	}

	// add every key to a slice.
	for k, _ := range c.bucket.value {
		c.keys = append(c.keys, k)
	}

	// sort the keys lexicographically.
	sort.Strings(c.keys)

	return c
}
