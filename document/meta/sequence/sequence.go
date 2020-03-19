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
	// ErrSequenceClosed used by Sequence
	ErrSequenceClosed = errors.New("sequence closed")
)

// Sequence for common ops between StringSequence and HashSequence
type Sequence struct {
	sync.Mutex
	closed           bool
	next             int64
	leased           int64
	bandwidth        int64
	putbacks         []int64
	renewLeaseFunc   func(step int64) (err error)
	updateLeasedFunc func() error
	clearFunc        func() error
}

// ReNew effectually creates another Sequence
func (seq *Sequence) ReNew() (err error) {
	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	err = seq.renewLeaseFunc(0)
	if err != nil {
		return
	}

	return
}

func (seq *Sequence) checkStatus() (err error) {
	if seq.closed {
		err = ErrSequenceClosed
	}
	return
}

// Close sequence
func (seq *Sequence) Close(releaseRemaining bool) (err error) {
	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	seq.closed = true
	if releaseRemaining {
		err = seq.releaseRemainingLocked()
	}
	return
}

// ReleaseRemaining for release the remaining sequence to avoid wasted integers.
func (seq *Sequence) ReleaseRemaining() (err error) {
	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	err = seq.releaseRemainingLocked()
	return
}

func (seq *Sequence) releaseRemainingLocked() (err error) {
	if seq.leased == seq.next {
		return
	}

	err = seq.updateLeasedFunc()
	return
}

// Clear sequence totally
func (seq *Sequence) Clear() (err error) {
	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	err = seq.clearFunc()
	return
}

// PutBack for reuse
func (seq *Sequence) PutBack(vals ...int64) {
	seq.Lock()
	defer seq.Unlock()

	seq.putbacks = append(seq.putbacks, vals...)
}

// Next would return the next integer in the sequence, updating the lease by running a transaction
// if needed.
func (seq *Sequence) Next() (val int64, err error) {
	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	if len(seq.putbacks) > 0 {
		val = seq.putbacks[len(seq.putbacks)-1]
		seq.putbacks = seq.putbacks[0 : len(seq.putbacks)-1]
		return
	}

	if seq.next >= seq.leased {
		err = seq.renewLeaseFunc(0)
		if err != nil {
			return
		}
	}

	seq.next++
	val = seq.next
	return
}

// IDRange for integers in (start, end]
type IDRange struct {
	Start int64
	End   int64
}

// NextN return n consecutive id in ranges
// when succeed, len(ranges) should be either 1 or 2
func (seq *Sequence) NextN(n int64) (ranges []IDRange, err error) {
	if n == 0 {
		return
	}

	seq.Lock()
	defer seq.Unlock()

	err = seq.checkStatus()
	if err != nil {
		return
	}

	// TODO pick up putbacks

	if seq.next+n <= seq.leased {
		end := seq.next + n
		r := IDRange{Start: seq.next, End: end}
		ranges = []IDRange{r}
		seq.next = end
		return
	}

	nextCopy := seq.next
	remain := seq.leased - nextCopy
	step := n + seq.bandwidth - remain
	err = seq.renewLeaseFunc(step)
	if err != nil {
		return
	}

	if remain > 0 {
		r := IDRange{Start: nextCopy, End: nextCopy + remain}
		ranges = []IDRange{r}
	}

	r := IDRange{Start: seq.next, End: seq.next + n - remain}
	ranges = append(ranges, r)

	return
}
