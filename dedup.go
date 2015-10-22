// depdup: A Streaming Deduplication package
//
// This package implements streaming deduplication, allowing you to remove duplicated data in streams.
// It implements variable block sizes and automatic content block adaptation.
// It has a fully streaming mode and an indexed mode, that has significantly reduced memory requirements.
//
// Read for an introduction to deduplication: https://blog.klauspost.com/fast-stream-deduplication-in-go
//
// Package home: https://github.com/klauspost/dedup
//
// Godoc: https://godoc.org/github.com/klauspost/dedup
//
package dedup

import (
	hasher "crypto/sha1"
	"fmt"
	"math/big"
)

// Returns an approximate Birthday probability calculation
// based on the number of blocks given and the hash size.
//
// It uses the simplified calculation:  p = k(k-1) / (2N)
//
// From http://preshing.com/20110504/hash-collision-probabilities/
func BirthdayProblem(blocks int) string {
	k := big.NewInt(int64(blocks))
	km1 := big.NewInt(int64(blocks - 1))
	ksq := k.Mul(k, km1)
	n := big.NewInt(0)
	n = n.Exp(big.NewInt(2), big.NewInt(int64(hasher.Size)*8), nil)
	twoN := n.Add(n, n)
	var t, t2 big.Rat
	var res *big.Rat
	//
	res = t.SetFrac(ksq, twoN)
	f64, _ := res.Float64()
	inv := t2.Inv(res).FloatString(0)
	invs := fmt.Sprintf(" ~ 1/%s ~ %v", inv, f64)

	return "Collision probability is" + invs
}
