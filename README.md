# dedup
A Streaming Deduplication package for Go

This package implements streaming deduplication, allowing you to remove duplicated data in streams. It implements variable block sizes and automatic content block adaptation. It has a fully streaming mode and an indexed mode, that has significantly reduced memory requirements.

For an introduction to deduplication read this blog post [Fast Stream Deduplication in Go](https://blog.klauspost.com/fast-stream-deduplication-in-go/).

Package home: https://github.com/klauspost/dedup

Godoc: https://godoc.org/github.com/klauspost/dedup

[![Build Status](https://travis-ci.org/klauspost/dedup.svg?branch=master)](https://travis-ci.org/klauspost/dedup)
[![GoDoc][1]][2]

[1]: https://godoc.org/github.com/klauspost/dedup?status.svg
[2]: https://godoc.org/github.com/klauspost/dedup

# Installation
To get the package use the standard:
```bash
go get -u github.com/klauspost/dedup
```

# Usage

If you haven't already, you should read the [LINK WHEN READY: Fast Streaming Deduplication in Go](https://blog.klauspost.com) blog post, since it will introduce different aspects and help you make choices for your setup.

There are two symmetric functions [`NewWriter`](https://godoc.org/github.com/klauspost/dedup#NewWriter)/[`NewReader`](https://godoc.org/github.com/klauspost/dedup#NewReader) and [`NewStreamWriter`](https://godoc.org/github.com/klauspost/dedup#NewStreamWriter)/[`NewStreamReader`](https://godoc.org/github.com/klauspost/dedup#NewStreamReader)`. The first pair creates an *indexed* stream, which will write the index and data to two separate streams. This allows to decode the deduplicated stream with much less memory. The second pair will write all data to a *single stream*. This allows for on-the-fly transfers, but will require more memory in the receiving end.

When you create a deduplicating stream, you can specify between *fixed* or *dynamic* block sizes. The dynamic blocks adapt block splits to the incoming content, but is slower than fixed size, and has to use more conservative memory estimations.

Here is an example of a full roundtrip with indexed streams. For more examples see the [godoc examples](https://godoc.org/github.com/klauspost/dedup#pkg-examples).

```Go
package main

import (
  "bytes"
  "fmt"
  "io"
  
  "github.com/klauspost/dedup"
)

// This will deduplicate a buffer of zeros to an indexed stream
func main() {
	// We will write out deduplicated data to these
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 50000))

	// Create a new writer, with each block being 1000 bytes fixed size.
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, 1000, 0)
	if err != nil {
		panic(err)
	}
	// Copy our input to the writer.
	io.Copy(w, input)
	
	// Close to flush the remaining buffers
	err = w.Close()
	if err != nil {
		panic(err)
	}

	// Create a new indexed stream reader:
	r, err := dedup.NewReader(&idx, &data)
	if err != nil {
		panic(err)
	}

	// Inspect how much memory it will use.
	fmt.Println("Memory use:", r.MaxMem())

	var dst bytes.Buffer

	// Read everything
	_, err = io.Copy(&dst, r)
	if err != nil && err != io.EOF {
		panic(err)
	}

	// Let us inspect what was written:
	fmt.Println("Returned data length:", dst.Len())
	fmt.Println("Everything zero:", 0 == bytes.Compare(dst.Bytes(), make([]byte, 50000)))
}
```

Note that there is no error resilience built in. If any data is corrupted in any way, it will probably not be detected, and there is no way to recover corrupted data. So if you are in an environment where that could occur, you should add additional checks to ensure that data is recoverable.

## Hash collisions

The encoder uses SHA-1 to identify and "remember" unique blocks. No hash is secure from collisions, but SHA-1 offers 160 bits of entropy. 

For example, the chance of a random hash collision to occur when encoding 1 TB data in 1KB blocks is 3.94×10^-31 : 1, or one in "2.5 thousand billion billion billion". This of course assumes a uniform hash distribution and no deliberate hash collision attacks.

If SHA-1 doesn't provide sufficient security, it has been made very easy for you to create a stronger version. It is possible for you to create a stronger version by simply changing the import:

```Go
import 	hasher "crypto/sha1"
```
You can use [sha256](https://golang.org/pkg/crypto/sha256/), [sha512](https://golang.org/pkg/crypto/sha512/) for stronger hashes, or [md5](https://golang.org/pkg/crypto/md5/) for a faster hash.

To help you calculate the birthday problem likelyhood with a given number of blocks, I have provided the [BirthdayProblem function](https://godoc.org/github.com/klauspost/dedup#BirthdayProblem).

## Why is this not compression?

Deduplication does the same as compression but on a higher level. Instead of looking for small matches, it attempts to find the "bigger" matches. It will attempt to match and eliminate blocks where all content matches. 

This can be useful when backing up disk images or other content where you have duplicated files, etc.

Deduplication is a good step *before* compression. You will still be able to compress your data, since unique blocks are passed through as-is, in order and without any modification.

# License

This code is published under an MIT license. See LICENSE file for more information.
