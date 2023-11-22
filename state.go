package tempdb

type BucketID uint64

type State struct {
	buckets []Bucket
	next    BucketID
	nextTX  int
}

const RootBucketID = BucketID(0)

func (s *State) Add(bkt Bucket) BucketID {
	s.next++
	bkt.id = s.next
	s.buckets = append(s.buckets, bkt)

	return bkt.id
}

// perform a deep copy.
func (s *State) Copy() *State {
	// create a new state.
	cpy := &State{
		next:   s.next,
		nextTX: s.nextTX,
	}

	for _, bkt := range s.buckets {
		// create a new bucket, shallow-copying most values.
		cbkt := Bucket{
			id:     bkt.id,
			parent: bkt.parent,

			key:      bkt.key,
			sequence: bkt.sequence,
		}

		// create a new map for storing the value.
		cbkt.value = make(map[string][]byte)

		// deep copy the map values.
		for k, v := range bkt.value {
			cbkt.value[k] = v
		}

		// add the bucket to the new state.
		cpy.buckets = append(cpy.buckets, cbkt)
	}

	return cpy
}
