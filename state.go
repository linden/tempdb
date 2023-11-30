package tempdb

type BucketID uint64

type State struct {
	Buckets []Bucket
	next    BucketID
	nextTX  int
}

const RootBucketID = BucketID(0)

func (s *State) Add(bkt Bucket) BucketID {
	s.next++
	bkt.ID = s.next
	s.Buckets = append(s.Buckets, bkt)

	return bkt.ID
}

// perform a deep copy.
func (s *State) Copy() *State {
	// create a new state.
	cpy := &State{
		next:   s.next,
		nextTX: s.nextTX,
	}

	for _, bkt := range s.Buckets {
		// create a new bucket, shallow-copying most values.
		cbkt := Bucket{
			ID:     bkt.ID,
			Parent: bkt.Parent,

			Key:      bkt.Key,
			sequence: bkt.sequence,
		}

		// create a new map for storing the value.
		cbkt.Value = make(map[string][]byte)

		// deep copy the map values.
		for k, v := range bkt.Value {
			cbkt.Value[k] = v
		}

		// add the bucket to the new state.
		cpy.Buckets = append(cpy.Buckets, cbkt)
	}

	return cpy
}
