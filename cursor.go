package tempdb

import (
	"cmp"
	"errors"
	"slices"
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
	Logger.Debug("cursor first", "bucket ID", c.bucket.id)

	return c.get(0)
}

func (c *Cursor) Last() ([]byte, []byte) {
	Logger.Debug("cursor last", "bucket ID", c.bucket.id, "index", len(c.keys)-1)

	return c.get(len(c.keys) - 1)
}

func (c *Cursor) Next() ([]byte, []byte) {
	Logger.Debug("cursor next", "bucket ID", c.bucket.id, "index", c.index+1)

	c.index++
	return c.get(c.index)
}

func (c *Cursor) Prev() ([]byte, []byte) {
	Logger.Debug("cursor prev", "bucket ID", c.bucket.id, "index", c.index-1)

	return c.get(c.index - 1)
}

func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	Logger.Debug("cursor seek", "bucket ID", c.bucket.id, "seek", seek)

	for i, k := range c.keys {
		// check if the key is >= to seek.
		if cmp.Compare(k, string(seek)) >= 0 {
			// move to the index.
			c.index = i
			break
		}
	}

	return c.get(c.index)
}

func (c *Cursor) Delete() error {
	Logger.Debug("cursor delete", "bucket ID", c.bucket.id, "index", c.index)

	k, _ := c.get(c.index)
	if k == nil {
		return errors.New("current index out of range")
	}

	return c.bucket.Delete(k)
}

func newCursor(bkt *Bucket) *Cursor {
	Logger.Debug("new cursor", "bucket ID", bkt.id, "transaction ID", bkt.tx.id)

	c := &Cursor{
		bucket: bkt,
	}

	// add every key to a slice.
	for k, _ := range c.bucket.value {
		c.keys = append(c.keys, k)
	}

	// sort the keys lexicographically.
	slices.Sort(c.keys)

	return c
}
