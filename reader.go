package dedup

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

type reader struct {
	blks    io.Writer
	idx     io.Writer
	size    int
	index   map[[sha1.Size]byte]struct{}
	input   chan block
	exit    chan struct{}
	exited  chan struct{}
	cur     []byte
	off     int
	buffers sync.Pool
}

type block struct {
	data     []byte
	sha1hash [sha1.Size]byte
}

var ErrSizeTooSmall = errors.New("block size too small")

func NewWriter(blocks io.Writer, index io.Writer, size uint) (io.WriteCloser, error) {
	r := &reader{
		blks:   blocks,
		idx:    index,
		size:   int(size),
		index:  make(map[[sha1.Size]byte]struct{}),
		input:  make(chan block, 10),
		exit:   make(chan struct{}, 0),
		exited: make(chan struct{}, 0),
		cur:    make([]byte, size),
	}
	r.buffers.New = func() interface{} { return make([]byte, size) }

	if r.size <= sha1.Size {
		return nil, ErrSizeTooSmall
	}
	var buf = make([]byte, 50)
	n := binary.PutUvarint(buf, uint64(size))
	_, err := index.Write(buf[:n])
	if err != nil {
		return nil, err
	}
	go r.hasher()
	return r
}

func (r *reader) Write(b []byte) (n int, err error) {
	written := 0
	for len(b) > 0 {
		n := copy(r.cur[r.off:], b)
		b = b[n:]
		r.off += n
		written += n
		if r.off == r.size {
			r.input <- block{data: r.cur}
			r.cur = r.buffers.Get().([]byte)
			r.off = 0
		}
	}
	return written, nil
}

var emptyHash = [20]byte{}

func (r *reader) Close() (err error) {
	select {
	case <-r.exited:
		return nil
	default:
	}
	close(r.exit)
	<-r.exited

	buf := bytes.NewBuffer(emptyHash[:])
	n, err := io.Copy(r.idx, &buf)
	if err != nil {
		return err
	}
	if n != 20 {
		return errors.New("short copy")
	}
	buf = bytes.NewBuffer(r.cur)
	n, err = io.Copy(r.blks, &buf)
	if err != nil {
		return err
	}
	if n != 20 {
		return errors.New("short copy")
	}

	return nil
}

func (r *reader) hasher() {
	hasher := sha1.New()
	defer close(r.exited)
	for b := range r.input {
		hasher.Reset()
		copy(b.sha1hash[:], hasher.Sum(b.data))
		_, ok := r.index[b.sha1hash]
		if !ok {
			r.index[b.sha1hash] = struct{}{}
			buf := bytes.NewBuffer(b.data)
			n, err := io.Copy(r.blks, buf)
			if err != nil {
				panic(err.Error())
			}
			if n != len(b.data) {
				panic("short write")
			}
		}
		n, err := r.idx.Write(b.sha1hash[:])
		if err != nil {
			panic(err.Error())
		}
		if n != len(b.data) {
			panic("short write")
		}
		r.buffers.Put(b.data)
	}
}
