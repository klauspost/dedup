package dedup

import (
	"bytes"
	hasher "crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"runtime"
	"sync"

	"github.com/klauspost/dedup/sort"
)

type Writer interface {
	io.WriteCloser

	// Split content, so a new block begins with next write
	Split()

	// MemUse returns an approximate maximum memory use in bytes for
	// encoder (Writer) and decoder (Reader) for the given number of bytes.
	MemUse(bytes int) (encoder, decoder int64)
}

// Size of the underlying hash in bytes for those interested
const HashSize = hasher.Size

// The smallest "maximum" block size allowed
const MinBlockSize = 512

// Deduplication mode used to determine how input is split.
type Mode int

const (
	// Fixed block size
	//
	// This is by far the fastest mode, and checks for duplicates
	// In fixed block sizes.
	// It can be helpful to use the "Split" function to reset offset, which
	// will reset duplication search at the position you are at.
	ModeFixed Mode = iota

	// Dynamic block size.
	//
	// This mode will create a deduplicator that will split the contents written
	// to it into dynamically sized blocks.
	// The size given indicates the maximum block size. Average size is usually maxSize/8.
	// Minimum block size is maxSize/128.
	ModeDynamic

	// Dynamic block size, including split on file signatures
	ModeDynamicSignatures

	// Dynamic block size only split on file signatures
	ModeSignaturesOnly
)

type writer struct {
	blks      io.Writer                 // Block data writer
	idx       io.Writer                 // Index writer
	maxSize   int                       // Maximum Block size
	maxBlocks int                       // Maximum backreference distance
	index     map[[hasher.Size]byte]int // Known hashes and their index
	input     chan *block               // Channel containing blocks to be hashed
	write     chan *block               // Channel containing (ordered) blocks to be written
	exited    chan struct{}             // Closed when the writer exits.
	cur       []byte                    // Current block being written
	off       int                       // Write offset in current block
	buffers   chan *block               // Buffers ready for re-use.
	vari64    []byte                    // Temporary buffer for writing varints
	err       error                     // Error state
	mu        sync.Mutex                // Mutex for error state
	nblocks   int                       // Current block number. First block is 1.
	writer    func(*writer, []byte) (int, error)
	flush     func(*writer) error
	close     func(*writer) error
}

// block contains information about a single block
type block struct {
	data     []byte
	sha1Hash [hasher.Size]byte
	hashDone chan error
	N        int
}

// ErrSizeTooSmall is returned if the requested block size is smaller than
// hash size.
var ErrSizeTooSmall = errors.New("maximum block size too small. must be at least 512 bytes")

// NewWriter will create a deduplicator that will split the contents written
// to it into blocks and de-duplicate these.
// The output is delivered as two streams, an index stream and a block stream.
// The index stream will contain information about which blocks are deduplicated
// and the block stream will contain uncompressed data blocks.
// The maximum memory use of the decoder is maxSize*maxBlocks.
// Set maxBlocks to 0 to disable decoder memory limit.
// This function returns data that is compatible with the NewReader function.
// The returned writer must be closed to flush the remaining data.
func NewWriter(index io.Writer, blocks io.Writer, mode Mode, maxSize, maxBlocks uint) (Writer, error) {
	ncpu := runtime.GOMAXPROCS(0)
	// For small block sizes we need to keep a pretty big buffer to keep input fed.
	// Constant below appears to be sweet spot measured with 4K blocks.
	var bufmul = 256 << 10 / int(maxSize)
	if bufmul < 2 {
		bufmul = 2
	}
	if maxBlocks < 0 {
		maxBlocks = 0
	}

	w := &writer{
		blks:      blocks,
		idx:       index,
		maxSize:   int(maxSize),
		index:     make(map[[hasher.Size]byte]int),
		input:     make(chan *block, ncpu*bufmul),
		write:     make(chan *block, ncpu*bufmul),
		exited:    make(chan struct{}, 0),
		cur:       make([]byte, maxSize),
		vari64:    make([]byte, binary.MaxVarintLen64),
		buffers:   make(chan *block, ncpu*bufmul),
		nblocks:   1,
		maxBlocks: int(maxBlocks),
	}

	switch mode {
	case ModeFixed:
		fw := &fixedWriter{}
		w.writer = fw.write
	case ModeDynamic:
		zw := newZpaqWriter(maxSize)
		w.writer = zw.write
	case ModeDynamicSignatures:
		zw := newZpaqWriter(maxSize)
		w.writer = zw.writeFile
	case ModeSignaturesOnly:
		w.writer = fileSplitOnly
	default:
		return nil, fmt.Errorf("dedup: unknown mode")
	}

	if w.maxSize < MinBlockSize {
		return nil, ErrSizeTooSmall
	}

	w.close = idxClose
	w.putUint64(1)               // Format
	w.putUint64(uint64(maxSize)) // Maximum block size

	// Start one goroutine per core
	for i := 0; i < ncpu; i++ {
		go w.hasher()
	}
	// Insert the buffers we will use
	for i := 0; i < ncpu*bufmul; i++ {
		w.buffers <- &block{data: make([]byte, maxSize), hashDone: make(chan error, 1)}
	}
	go w.blockWriter()
	return w, nil
}

// ErrSizeTooSmall is returned if the requested block size is smaller than
// hash size.
var ErrMaxBlocksTooSmall = errors.New("there must be at least 1 block backreference")

// NewStreamWriter will create a deduplicator that will split the contents written
// to it into blocks and de-duplicate these.
// The output is delivered as a single stream, and memory use will remain stable for
// both writing and reading the stream.
// This function returns data that is compatible with the NewStreamReader function.
// You must specify the maximum number of blocks to keep in memory.
// The maximum memory use of the decoder is maxSize*maxBlocks.
// The returned writer must be closed to flush the remaining data.
func NewStreamWriter(out io.Writer, mode Mode, maxSize, maxBlocks uint) (Writer, error) {
	ncpu := runtime.GOMAXPROCS(0)
	// For small block sizes we need to keep a pretty big buffer to keep input fed.
	// Constant below appears to be sweet spot measured with 4K blocks.
	var bufmul = 256 << 10 / int(maxSize)
	if bufmul < 2 {
		bufmul = 2
	}
	if maxBlocks < 1 {
		return nil, ErrMaxBlocksTooSmall
	}
	w := &writer{
		idx:       out,
		maxSize:   int(maxSize),
		index:     make(map[[hasher.Size]byte]int),
		input:     make(chan *block, ncpu*bufmul),
		write:     make(chan *block, ncpu*bufmul),
		exited:    make(chan struct{}, 0),
		cur:       make([]byte, maxSize),
		vari64:    make([]byte, binary.MaxVarintLen64),
		buffers:   make(chan *block, ncpu*bufmul),
		nblocks:   1,
		maxBlocks: int(maxBlocks),
	}

	switch mode {
	case ModeFixed:
		fw := &fixedWriter{}
		w.writer = fw.write
	case ModeDynamic:
		zw := newZpaqWriter(maxSize)
		w.writer = zw.write
	case ModeDynamicSignatures:
		zw := newZpaqWriter(maxSize)
		w.writer = zw.writeFile
	case ModeSignaturesOnly:
		w.writer = fileSplitOnly
	default:
		return nil, fmt.Errorf("dedup: unknown mode")
	}

	if w.maxSize < MinBlockSize {
		return nil, ErrSizeTooSmall
	}

	w.close = streamClose
	w.putUint64(2)                 // Format
	w.putUint64(uint64(maxSize))   // Maximum block size
	w.putUint64(uint64(maxBlocks)) // Maximum backreference length

	// Start one goroutine per core
	for i := 0; i < ncpu; i++ {
		go w.hasher()
	}
	// Insert the buffers we will use
	for i := 0; i < ncpu*bufmul; i++ {
		w.buffers <- &block{data: make([]byte, maxSize), hashDone: make(chan error, 1)}
	}
	go w.blockStreamWriter()
	return w, nil
}

// putUint64 will Write a uint64 value to index stream.
func (w *writer) putUint64(v uint64) error {
	n := binary.PutUvarint(w.vari64, v)
	n2, err := w.idx.Write(w.vari64[:n])
	if err != nil {
		return err
	}
	if n2 != n {
		return io.ErrShortWrite
	}
	return nil
}

// Split content, so a new block begins with next write
func (w *writer) Split() {
	if w.off == 0 {
		return
	}
	b := <-w.buffers
	// Swap block with current
	w.cur, b.data = b.data[:w.maxSize], w.cur[:w.off]
	b.N = w.nblocks

	w.input <- b
	w.write <- b
	w.nblocks++
	w.off = 0
}

// Write contents to the deduplicator.
func (w *writer) Write(b []byte) (n int, err error) {
	return w.writer(w, b)
}

// setErr will set the error state of the writer.
func (w *writer) setErr(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

// idxClose will flush the remainder of an index based stream
func idxClose(w *writer) (err error) {
	// Insert length of remaining data into index
	w.putUint64(uint64(math.MaxUint64))
	w.putUint64(uint64(w.maxSize - w.off))

	buf := bytes.NewBuffer(w.cur[0:w.off])
	n, err := io.Copy(w.blks, buf)
	if err != nil {
		return err
	}
	if int(n) != w.off {
		return errors.New("idxClose: r.cur short write")
	}
	return nil
}

// streamClose will flush the remainder of an single stream
func streamClose(w *writer) (err error) {
	// Insert length of remaining data into index
	w.putUint64(uint64(math.MaxUint64))
	w.putUint64(uint64(w.maxSize - w.off))

	buf := bytes.NewBuffer(w.cur[0:w.off])
	n, err := io.Copy(w.idx, buf)
	if err != nil {
		return err
	}
	if int(n) != w.off {
		return errors.New("streamClose: r.cur short write")
	}
	return nil
}

// Close and flush the remaining data to output.
func (w *writer) Close() (err error) {
	select {
	case <-w.exited:
		return nil
	default:
	}
	if w.flush != nil {
		err := w.flush(w)
		if err != nil {
			return err
		}
	}
	close(w.input)
	close(w.write)
	<-w.exited

	if w.close != nil {
		err := w.close(w)
		if err != nil {
			return err
		}
	}

	return nil
}

// hasher will hash incoming blocks
// and signal the writer when done.
func (w *writer) hasher() {
	h := hasher.New()
	for b := range w.input {
		buf := bytes.NewBuffer(b.data)
		h.Reset()
		n, err := io.Copy(h, buf)
		if err != nil {
			w.setErr(err)
			return
		}
		if int(n) != len(b.data) {
			panic("short write monkey")
		}
		_ = h.Sum(b.sha1Hash[:0])
		b.hashDone <- nil
	}
}

// blockWriter will write hashed blocks to the output
// and recycle the buffers.
func (w *writer) blockWriter() {
	defer close(w.exited)

	sortA := make([]int, w.maxBlocks+1)

	for b := range w.write {
		_ = <-b.hashDone
		match, ok := w.index[b.sha1Hash]
		if !ok {
			buf := bytes.NewBuffer(b.data)
			n, err := io.Copy(w.blks, buf)
			if err != nil {
				w.setErr(err)
				return
			}
			if int(n) != len(b.data) {
				panic("short write on copy")
			}
			w.putUint64(0)
			w.putUint64(uint64(w.maxSize) - uint64(n))
		} else {
			offset := b.N - match
			if offset <= 0 {
				panic("negative offset, should be impossible")
			}
			w.putUint64(uint64(offset))
		}
		// Update hash to latest match
		w.index[b.sha1Hash] = b.N

		// Purge the entries with the oldest matches
		if w.maxBlocks > 0 && len(w.index) > w.maxBlocks {
			ar := sortA[0:len(w.index)]
			i := 0
			for _, v := range w.index {
				ar[i] = v
				i++
			}
			sort.Asc(ar)
			// Cut the oldest quarter blocks
			// since this isn't free
			ar = ar[:w.maxBlocks/4]
			for k, v := range w.index {
				for _, val := range ar {
					if v == val {
						delete(w.index, k)
						break
					}
				}
			}
		}

		// Done, reinsert buffer
		w.buffers <- b
	}
}

// blockStreamWriter will write blocks and indexes to the output stream
// and recycle the buffers.
func (w *writer) blockStreamWriter() {
	defer close(w.exited)
	for b := range w.write {
		_ = <-b.hashDone
		match, ok := w.index[b.sha1Hash]
		if w.maxBlocks > 0 && (b.N-match) > w.maxBlocks {
			ok = false
		}
		if !ok {
			w.putUint64(0)
			w.putUint64(uint64(w.maxSize) - uint64(len(b.data)))
			buf := bytes.NewBuffer(b.data)
			n, err := io.Copy(w.idx, buf)
			if err != nil {
				w.setErr(err)
				return
			}
			if int(n) != len(b.data) {
				panic("short write")
			}
		} else {
			offset := b.N - match
			if offset <= 0 {
				panic("negative offset, should be impossible")
			}
			w.putUint64(uint64(offset))
		}
		// Update hash to latest match
		w.index[b.sha1Hash] = b.N

		// Purge old entries once in a while
		if w.maxBlocks > 0 && b.N&127 == 127 {
			for k, v := range w.index {
				if (v - match) > w.maxBlocks {
					delete(w.index, k)
				}
			}
		}
		// Done, reinsert buffer
		w.buffers <- b
	}
}

type fixedWriter struct{}

// Write blocks of similar size.
func (f *fixedWriter) write(w *writer, b []byte) (n int, err error) {
	written := 0
	for len(b) > 0 {
		n := copy(w.cur[w.off:], b)
		b = b[n:]
		w.off += n
		written += n
		// Filled the buffer? Send it off!
		if w.off == w.maxSize {
			b := <-w.buffers
			// Swap block with current
			w.cur, b.data = b.data, w.cur
			b.N = w.nblocks

			w.input <- b
			w.write <- b
			w.nblocks++
			w.off = 0
		}
	}
	return written, nil
}

// Returns an approximate Birthday probability calculation
// It uses the simplified calculation:
//
// p = k(k-1) / (2N)
//
// From http://preshing.com/20110504/hash-collision-probabilities/
func BirthDayProblem(blocks int) string {
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

// MemUse returns an approximate maximum memory use in bytes for
// encoder (Writer) and decoder (Reader) for the given number of bytes.
func (w *writer) MemUse(bytes int) (encoder, decoder int64) {
	blocks := (bytes + w.maxSize - 1) / w.maxSize
	if w.maxBlocks > 0 {
		if w.maxBlocks < blocks {
			blocks = w.maxBlocks
		}
	}
	// Data length
	data := big.NewInt(int64(blocks))
	data = data.Mul(data, big.NewInt(int64(w.maxSize)))
	d := data.Int64()
	if data.BitLen() > 63 {
		d = math.MaxInt64
	}
	// Index length
	bl := big.NewInt(int64(blocks))
	perBlock := big.NewInt(int64(HashSize + 8 /*int64*/ + 24 /* map entry*/))
	total := bl.Mul(bl, perBlock)
	if total.BitLen() > 63 {
		return math.MaxInt64, d
	}
	return total.Int64(), d
}

// Split blocks like ZPAQ: (public domain)
type zpaqWriter struct {
	h           uint32 // rolling hash for finding fragment boundaries
	c1          byte   // last byte
	maxFragment int
	minFragment int
	maxHash     uint32
	o1          [256]byte // order 1 context -> predicted byte
}

// Split blocks. Typically block size will be maxSize / 8
// Minimum block size is maxSize/128.
//
// The break point is content dependent.
// Any insertions, deletions, or edits that occur before the start of the 32+ byte dependency window
// don't affect the break point.
// This makes it likely for two files to still have identical fragments far away from any edits.
func newZpaqWriter(maxSize uint) *zpaqWriter {
	fragment := math.Log2(float64(maxSize) / (64 * 8))
	mh := math.Exp2(22 - fragment)
	return &zpaqWriter{
		maxFragment: int(maxSize),
		minFragment: int(maxSize / 128),
		maxHash:     uint32(mh),
	}
}

// h is a 32 bit hash that depends on the last 32 bytes that were mispredicted by the order 1 model o1[].
// h < maxhash therefore occurs with probability 2^-16, giving an average fragment size of 64K.
// The variable size dependency window works because one constant is odd (correct prediction, no shift),
// and the other is even but not a multiple of 4 (missed prediction, 1 bit shift left).
// This is different from a normal Rabin filter, which uses a large fixed-sized dependency window
// and two multiply operations, one at the window entry and the inverse at the window exit.
func (z *zpaqWriter) write(r *writer, b []byte) (int, error) {
	c1 := z.c1
	for _, c := range b {
		if c == z.o1[c1] {
			z.h = (z.h + uint32(c) + 1) * 314159265
		} else {
			z.h = (z.h + uint32(c) + 1) * 271828182
		}
		z.o1[c1] = c
		c1 = c
		r.cur[r.off] = c
		r.off++

		// At a break point? Send it off!
		if (r.off >= z.minFragment && z.h < z.maxHash) || r.off >= z.maxFragment {
			b := <-r.buffers
			// Swap block with current
			r.cur, b.data = b.data[:r.maxSize], r.cur[:r.off]
			b.N = r.nblocks

			r.input <- b
			r.write <- b
			r.nblocks++
			r.off = 0
			z.h = 0
			c1 = 0
		}
	}
	z.c1 = c1
	return len(b), nil
}

// Split on zpaq hash, file signatures and maximum block size.
func (z *zpaqWriter) writeFile(w *writer, b []byte) (int, error) {
	c1 := z.c1

	for i, c := range b {
		split := false
		v := sigmap[c]
		if len(v) > 0 && i < len(b)-6 {
			for _, s := range v {
				if bytes.Compare(s, b[i+1:i+6]) == 0 {
					split = true
				}
			}
		}
		if c == z.o1[c1] {
			z.h = (z.h + uint32(c) + 1) * 314159265
		} else {
			z.h = (z.h + uint32(c) + 1) * 271828182
		}
		z.o1[c1] = c
		c1 = c
		w.cur[w.off] = c
		w.off++

		// Filled the buffer? Send it off!
		if w.off >= z.minFragment && (z.h < z.maxHash || split || w.off >= z.maxFragment) {
			b := <-w.buffers
			// Swap block with current
			w.cur, b.data = b.data[:w.maxSize], w.cur[:w.off]
			b.N = w.nblocks

			w.input <- b
			w.write <- b
			w.nblocks++
			w.off = 0
			z.h = 0
			c1 = 0
		}
	}
	z.c1 = c1
	return len(b), nil
}

// Split on maximum size and file signatures only.
func fileSplitOnly(w *writer, b []byte) (int, error) {
	for i, c := range b {
		split := false
		v := sigmap[c]
		if len(v) > 0 && i < len(b)-6 {
			for _, s := range v {
				if bytes.Compare(s, b[i+1:i+6]) == 0 {
					split = true
				}
			}
		}
		w.cur[w.off] = c
		w.off++

		// Filled the buffer? Send it off!
		if split || w.off >= w.maxSize {
			b := <-w.buffers
			// Swap block with current
			w.cur, b.data = b.data[:w.maxSize], w.cur[:w.off]
			b.N = w.nblocks

			w.input <- b
			w.write <- b
			w.nblocks++
			w.off = 0
		}
	}
	return len(b), nil
}

// 4 times faster than map[byte][][]byte
// 2 times faster than generated code (switch byte 0, if)
var sigmap [256][][]byte

func init() {
	for _, sig := range signatures {
		l := sig[0]
		slice := sig[1 : 1+l]
		x := sigmap[slice[0]]
		dst := make([]byte, l-1)
		copy(dst, slice[1:])
		x = append(x, dst)
		sigmap[slice[0]] = x
	}
}

// File start signatures
// 8 bytes, 1 byte length (1 to 7), 1-7 bytes identifier literals, 7-length padding.
var signatures = [][8]byte{
	[8]byte{3, 0x42, 0x5A, 0x68, 0, 0, 0, 0},             //bzip 2
	[8]byte{3, 0x1f, 0x8b, 0x00, 0, 0, 0, 0},             //gzip (store)
	[8]byte{3, 0x1f, 0x8b, 0x08, 0, 0, 0, 0},             //gzip (deflate)
	[8]byte{6, 0x47, 0x49, 0x46, 0x38, 0x37, 0x61, 0},    //GIF87a
	[8]byte{6, 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0},    //GIF89a
	[8]byte{4, 0x49, 0x49, 0x2A, 0x0, 0, 0, 0},           //TIFF
	[8]byte{4, 0x4D, 0x4D, 0x00, 0x2A, 0, 0, 0},          //TIFF
	[8]byte{3, 0xFF, 0xD8, 0xFF, 0, 0, 0, 0},             //JPEG
	[8]byte{4, 0x46, 0x4F, 0x52, 0x4D, 0, 0, 0},          //IFF (FORM)
	[8]byte{4, 0x50, 0x4B, 0x03, 0x04, 0, 0, 0},          //ZIP
	[8]byte{4, 0x50, 0x4B, 0x07, 0x08, 0, 0, 0},          //ZIP
	[8]byte{7, 0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00}, //RAR
	[8]byte{4, 0x7F, 0x45, 0x4C, 0x46, 0, 0, 0},          //ELF
	[8]byte{7, 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A}, //PNG
	[8]byte{4, 0xCA, 0xFE, 0xBA, 0xBE, 0, 0, 0},          //Java Class
	[8]byte{3, 0xEF, 0xBB, 0xBF, 0, 0, 0, 0},             //Unicode byte order mark
	[8]byte{4, 0xFE, 0xED, 0xFA, 0xCE, 0, 0, 0},          //Mach-O binary (32-bit)
	[8]byte{4, 0xFE, 0xED, 0xFA, 0xCF, 0, 0, 0},          //Mach-O binary (64-bit)
	[8]byte{4, 0xCE, 0xFA, 0xED, 0xFE, 0, 0, 0},          //Mach-O binary (32-bit)
	[8]byte{4, 0xCF, 0xFA, 0xED, 0xFE, 0, 0, 0},          //Mach-O binary (64-bit)
	[8]byte{4, 0xFF, 0xFE, 0x00, 0x00, 0, 0, 0},          //BOM 32-bit Unicode Transfer Format
	[8]byte{4, 0x50, 0x45, 0x00, 0x00, 0, 0, 0},          //PE (PE Header)
	[8]byte{4, 0x25, 0x21, 0x50, 0x53, 0, 0, 0},          //PS
	[8]byte{4, 0x25, 0x50, 0x44, 0x46, 0, 0, 0},          //PDF
	[8]byte{7, 0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF}, //ASF
	[8]byte{7, 0xA6, 0xD9, 0x00, 0xAA, 0x00, 0x62, 0xCE}, //WMV
	[8]byte{7, 0x24, 0x53, 0x44, 0x49, 0x30, 0x30, 0x30}, //SDI
	[8]byte{4, 0x4F, 0x67, 0x67, 0x53, 0, 0, 0},          //OGG
	[8]byte{4, 0x38, 0x42, 0x50, 0x53, 0, 0, 0},          //PSD
	[8]byte{4, 0x52, 0x49, 0x46, 0x46, 0, 0, 0},          //WAV/AVI
	[8]byte{3, 0x49, 0x44, 0x33, 0, 0, 0, 0},             //MP3 (ID3 v2, all versions)
	[8]byte{5, 0x43, 0x44, 0x30, 0x30, 0x31, 0, 0},       //ISO
	[8]byte{3, 0x4B, 0x44, 0x4D, 0, 0, 0, 0},             //VMDK
	[8]byte{4, 0x66, 0x4C, 0x61, 0x43, 0, 0, 0},          //FLAC
	[8]byte{4, 0x4D, 0x54, 0x68, 0x64, 0, 0, 0},          //MIDI
	[8]byte{5, 0x1A, 0x45, 0xDF, 0xA3, 0, 0},             //MKV
	[8]byte{5, 0x1F, 0x43, 0xB6, 0x75, 0, 0},             //MKV Cluster
	[8]byte{4, 0x46, 0x4c, 0x56, 0x01, 0, 0, 0},          //FLV (old format)
	[8]byte{7, 0x66, 0x74, 0x79, 0x70, 0x33, 0x67, 0x70}, //3GG/MP4
	[8]byte{6, 0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c, 0},    //7zip
	[8]byte{6, 0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0},    //XZ format
	[8]byte{7, 0x42, 0x4f, 0x4f, 0x4b, 0x4d, 0x4f, 0x42}, //MOBI book format
	[8]byte{7, 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20}, //SQLite DB
	[8]byte{6, 0x7b, 0x5c, 0x72, 0x74, 0x66, 0x31, 0},    //RTF '{\rtf1\'
	[8]byte{7, '<', '!', 'D', 'O', 'C', 'T', 'Y'},        //HTML Doctype
	[8]byte{4, 0x49, 0x54, 0x53, 0x46, 0, 0, 0},          //CHM Fomrat
	[8]byte{6, '<', '?', 'x', 'm', 'l', ' ', 0},          //XML Doctype
	[8]byte{5, 0x2e, 0x70, 0x6e, 0x20, 0x30, 0, 0},       //troff page #0
	[8]byte{4, 0xfe, 0x62, 0x69, 0x6e, 0, 0, 0},          //MySQL binlog
	[8]byte{5, 'K', 'D', 'M', 'V', 0x01, 0, 0},           //Virtual machine disk image
	[8]byte{5, 'M', 'R', 'V', 'N', 0x01, 0, 0},           //VMware nvram image

	// Exotics:
	//[8]byte{7, 0x46, 0x55, 0x4a, 0x49, 0x46, 0x49, 0x4c}, //FUJI Raw format
	//[8]byte{7, 0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a}, //MSI format
	//[8]byte{5, 0x46, 0x4f, 0x56, 0x62, 0x00, 0, 0}, //X3F format

	//[8]byte{4, 0x50, 0x4B, 0x05, 0x06, 0, 0, 0},          //ZIP empty archive
}
