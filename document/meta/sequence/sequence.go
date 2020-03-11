package sequence

import (
	"errors"
	"sync"
)

var (
	// ErrEmptyKeywordForStringSequence used by StringSequence
	ErrEmptyKeywordForStringSequence = errors.New("keyword cannot be empty for string sequence")
	// ErrEmptyKeyForHashSequence usec by HashSequence
	ErrEmptyKeyForHashSequence = errors.New("key cannot be empty for hash sequence")
	// ErrEmptyFieldForHashSequence usec by HashSequence
	ErrEmptyFieldForHashSequence = errors.New("field cannot be empty for hash sequence")
	// ErrZeroBandwidth used by StringSequence/HashSequence
	ErrZeroBandwidth = errors.New("bandwidth must be greater than zero")
)

// Sequence for common ops between StringSequence and HashSequence
type Sequence struct {
	sync.Mutex
	next             int64
	leased           int64
	bandwidth        int64
	renewLeaseFunc   func(step int64) (err error)
	updateLeasedFunc func() error
	clearFunc        func() error
}

// ReNew effectually creates another Sequence
func (sc *Sequence) ReNew() (err error) {
	sc.Lock()
	defer sc.Unlock()

	err = sc.renewLeaseFunc(0)
	if err != nil {
		return
	}

	return
}

// ReleaseRemaining for release the remaining sequence to avoid wasted integers.
func (sc *Sequence) ReleaseRemaining() (err error) {
	sc.Lock()
	defer sc.Unlock()

	if sc.leased == sc.next {
		return
	}

	err = sc.updateLeasedFunc()
	return
}

// Clear sequence totally
func (sc *Sequence) Clear() (err error) {
	sc.Lock()
	defer sc.Unlock()

	err = sc.clearFunc()
	return
}

// Next would return the next integer in the sequence, updating the lease by running a transaction
// if needed.
func (sc *Sequence) Next() (val int64, err error) {
	sc.Lock()
	defer sc.Unlock()

	if sc.next >= sc.leased {
		err = sc.renewLeaseFunc(0)
		if err != nil {
			return
		}
	}

	sc.next++
	val = sc.next
	return
}

// IDRange for integers in (start, end]
type IDRange struct {
	Start int64
	End   int64
}

// NextN return n consecutive id in ranges
// when succeed, len(ranges) should be either 1 or 2
func (sc *Sequence) NextN(n int64) (ranges []IDRange, err error) {
	if n == 0 {
		return
	}

	sc.Lock()
	defer sc.Unlock()

	if sc.next+n <= sc.leased {
		end := sc.next + n
		r := IDRange{Start: sc.next, End: end}
		ranges = []IDRange{r}
		sc.next = end
		return
	}

	nextCopy := sc.next
	remain := sc.leased - nextCopy
	step := n + sc.bandwidth - remain
	err = sc.renewLeaseFunc(step)
	if err != nil {
		return
	}

	if remain > 0 {
		r := IDRange{Start: nextCopy, End: nextCopy + remain}
		ranges = []IDRange{r}
	}

	r := IDRange{Start: sc.next, End: sc.next + n - remain}
	ranges = append(ranges, r)

	return
}
