package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// MedianOf return the median of the given slice
func MedianOf(s []int64) int64 {
	if s == nil || len(s) == 0 {
		panic("input is nil or empty")
	}
	lo, hi := 0, len(s)
	posMid := len(s) / 2
	for {
		p := partition(s, lo, hi)
		if p == posMid {
			return s[p]
		} else if p < posMid {
			lo = p + 1
		} else {
			hi = p
		}
	}
}

func partition(s []int64, lo, hi int) int {
	pivot := s[hi-1]
	i := lo - 1
	for j := lo; j <= hi-1; j++ {
		if s[j] < pivot {
			i++
			s[i], s[j] = s[j], s[i]
		}
	}
	// fit the pivot to the right position
	s[i+1], s[hi-1] = s[hi-1], s[i+1]
	return i + 1
}
