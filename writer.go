package dedup

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"sync"
)

type writer struct {
	blks    io.Writer
	idx     io.Writer
	size    int
	index   map[[sha1.Size]byte]struct{}
	input   chan *block
	write   chan *block
	exited  chan struct{}
	cur     []byte
	off     int
	buffers sync.Pool
	vari64  []byte
	err     error
	mu      sync.Mutex
}

type block struct {
	data     []byte
	sha1Hash [sha1.Size]byte
	hashDone chan error
}

var ErrSizeTooSmall = errors.New("block size too small")

func NewWriter(index io.Writer, blocks io.Writer, size uint) (io.WriteCloser, error) {
	ncpu := runtime.GOMAXPROCS(0)
	r := &writer{
		blks:   blocks,
		idx:    index,
		size:   int(size),
		index:  make(map[[sha1.Size]byte]struct{}),
		input:  make(chan *block, ncpu*4),
		write:  make(chan *block, ncpu*4),
		exited: make(chan struct{}, 0),
		cur:    make([]byte, size),
		vari64: make([]byte, binary.MaxVarintLen64),
	}
	r.buffers.New = func() interface{} { return make([]byte, size) }

	if r.size <= sha1.Size {
		return nil, ErrSizeTooSmall
	}

	r.putUint64(1) // Format
	r.putUint64(uint64(size))

	// Start one goroutine per core
	for i := 0; i < ncpu; i++ {
		go r.hasher()
	}
	go r.writer()
	return r, nil
}

// putUint64 will Write uint64 value to index stream.
func (r *writer) putUint64(v uint64) error {
	n := binary.PutUvarint(r.vari64, v)
	n2, err := r.idx.Write(r.vari64[:n])
	if err != nil {
		return err
	}
	if n2 != n {
		return io.ErrShortWrite
	}
	return nil
}

func (r *writer) Write(b []byte) (n int, err error) {
	written := 0
	for len(b) > 0 {
		n := copy(r.cur[r.off:], b)
		b = b[n:]
		r.off += n
		written += n
		if r.off == r.size {
			b := &block{data: r.cur, hashDone: make(chan error, 1)}
			r.input <- b
			r.write <- b
			r.cur = r.buffers.Get().([]byte)
			r.off = 0
		}
	}
	return written, nil
}

var emptyHash = [sha1.Size]byte{}

func (r *writer) Close() (err error) {
	select {
	case <-r.exited:
		return nil
	default:
	}
	close(r.input)
	close(r.write)
	<-r.exited

	// Insert empty hash into index to indicate EOF
	buf := bytes.NewBuffer(emptyHash[:])
	n, err := io.Copy(r.idx, buf)
	if err != nil {
		return err
	}
	if n != sha1.Size {
		return errors.New("close short copy")
	}
	// Insert length of remaining data into index
	r.putUint64(uint64(r.off))

	buf = bytes.NewBuffer(r.cur[0:r.off])
	n, err = io.Copy(r.blks, buf)
	if err != nil {
		return err
	}
	if int(n) != r.off {
		return errors.New("r.cur short copy")
	}

	return nil
}

func (r *writer) hasher() {
	hasher := sha1.New()
	for b := range r.input {
		buf := bytes.NewBuffer(b.data)
		hasher.Reset()
		n, err := io.Copy(hasher, buf)
		if err != nil {
			panic(err.Error())
		}
		if int(n) != len(b.data) {
			panic("short write")
		}
		_ = hasher.Sum(b.sha1Hash[:0])
		b.hashDone <- nil
	}
}

func (r *writer) writer() {
	defer close(r.exited)
	for b := range r.write {
		_ = <-b.hashDone
		_, ok := r.index[b.sha1Hash]
		if !ok {
			r.index[b.sha1Hash] = struct{}{}
			buf := bytes.NewBuffer(b.data)
			n, err := io.Copy(r.blks, buf)
			if err != nil {
				panic(err.Error())
			}
			if int(n) != len(b.data) {
				panic("short write")
			}
		}
		n, err := r.idx.Write(b.sha1Hash[:])
		if err != nil {
			panic(err.Error())
		}
		if n != sha1.Size {
			panic("short write")
		}
		r.buffers.Put(b.data)
	}
}

/*
ZPAQ: (public domain)

  // Set block and fragment sizes
  // -fragment N     Set average dedupe fragment size = 2^N KiB (default: 6)
  if (fragment<0) fragment=0;

  const unsigned blocksize=(1u<<(20+atoi(method.c_str()+1)))-4096;
  const unsigned MAX_FRAGMENT=fragment>19 || (8128u<<fragment)>blocksize-12
      ? blocksize-12 : 8128u<<fragment;
  const unsigned MIN_FRAGMENT=fragment>25 || (64u<<fragment)>MAX_FRAGMENT
      ? MAX_FRAGMENT : 64u<<fragment;


		unsigned char o1[256]={0}; // order 1 context -> predicted byte
		int c=EOF;  // current byte
		int hits = 0;  // hits - we can use this to determine
        while (true) {
          c=in.get();
          if (c!=EOF) {
            if (c==o1[c1]) h=(h+c+1)*314159265u; hits++
            else h=(h+c+1)*271828182u;
            o1[c1]=c;
            c1=c;
            sha1.put(c);
            fragbuf[sz++]=c;
          	if ( sz>=MAX_FRAGMENT || (fragment<=22 && h<(1u<<(22-fragment)) && sz>=MIN_FRAGMENT))
            	break;
          } else {
          	break;
          }
        }

*/
