# dedup
Streaming de-duplication package

# IN DEVELOPMENT - do not use (yet).

[![Build Status](https://travis-ci.org/klauspost/dedup.svg?branch=master)](https://travis-ci.org/klauspost/dedup)
[![GoDoc][1]][2]

[1]: https://godoc.org/github.com/klauspost/dedup?status.svg
[2]: https://godoc.org/github.com/klauspost/dedup

## Why is this not compression?

Deduplication does the same as compression but on a higher level. Instead of looking for small matches, it attempts to find the "bigger" matches. It will attempt to match and eliminate blocks where all content matches. 

This can be useful when backing up disk images or other content where you have duplicated files, etc.

Deduplication is a good step *before* compression. You will still be able to compress your data, since unique blocks are passed through as-is, in order and without any modification.
