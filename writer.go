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
)

// Size of the underlying hash in bytes for those interested
const HashSize = hasher.Size

type fixedWriter struct {
	blks    io.Writer                 // Block data writer
	idx     io.Writer                 // Index writer
	size    int                       // Block size
	index   map[[hasher.Size]byte]int // Known hashes and their index
	input   chan *block               // Channel containing blocks to be hashed
	write   chan *block               // Channel containing (ordered) blocks to be written
	exited  chan struct{}             // Closed when the writer exits.
	cur     []byte                    // Current block being written
	off     int                       // Write offset in current block
	buffers chan *block               // Buffers ready for re-use.
	vari64  []byte                    // Temporary buffer for writing varints
	err     error                     // Error state
	mu      sync.Mutex                // Mutex for error state
	nblocks int                       // Current block number. First block is 1.
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
var ErrSizeTooSmall = errors.New("block size too small")

// NewFixed will create a deduplicator that will split the contents written
// to it into equally sized blocks and de-duplicate these.
// The output is delivered as two streams, an index stream and a block stream.
// The index stream will contain information about which blocks are deduplicated
// and the block stream will contain uncompressed data blocks.
// The returned writer must be closed to flush the remaining data.
func NewFixed(index io.Writer, blocks io.Writer, size uint) (io.WriteCloser, error) {
	ncpu := runtime.GOMAXPROCS(0)
	// For small block sizes we need to keep a pretty big buffer to keep input fed.
	// Constant below appears to be sweet spot measured with 4K blocks.
	var bufmul = 256 << 10 / int(size)
	if bufmul < 2 {
		bufmul = 2
	}
	r := &fixedWriter{
		blks:    blocks,
		idx:     index,
		size:    int(size),
		index:   make(map[[hasher.Size]byte]int),
		input:   make(chan *block, ncpu*bufmul),
		write:   make(chan *block, ncpu*bufmul),
		exited:  make(chan struct{}, 0),
		cur:     make([]byte, size),
		vari64:  make([]byte, binary.MaxVarintLen64),
		buffers: make(chan *block, ncpu*bufmul),
		nblocks: 1,
	}

	if r.size <= hasher.Size {
		return nil, ErrSizeTooSmall
	}

	r.putUint64(1) // Format
	r.putUint64(uint64(size))

	// Start one goroutine per core
	for i := 0; i < ncpu; i++ {
		go r.hasher()
	}
	// Insert the buffers we will use
	for i := 0; i < ncpu*bufmul; i++ {
		r.buffers <- &block{data: make([]byte, size), hashDone: make(chan error, 1)}
	}
	go r.writer()
	return r, nil
}

// putUint64 will Write uint64 value to index stream.
func (r *fixedWriter) putUint64(v uint64) error {
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

// Write contents to the deduplicator.
func (r *fixedWriter) Write(b []byte) (n int, err error) {
	written := 0
	for len(b) > 0 {
		n := copy(r.cur[r.off:], b)
		b = b[n:]
		r.off += n
		written += n
		// Filled the buffer? Send it off!
		if r.off == r.size {
			b := <-r.buffers
			// Swap block with current
			r.cur, b.data = b.data, r.cur
			b.N = r.nblocks

			r.input <- b
			r.write <- b
			r.nblocks++
			r.off = 0
		}
	}
	return written, nil
}

// setErr will set the error state of the writer.
func (r *fixedWriter) setErr(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

var emptyHash = [hasher.Size]byte{}

// Close and flush the remaining data to output.
func (r *fixedWriter) Close() (err error) {
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
	if n != hasher.Size {
		return errors.New("close short copy")
	}
	// Insert length of remaining data into index
	r.putUint64(uint64(math.MaxUint64))
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

// hasher will hash incoming blocks
// and signal the writer when done.
func (r *fixedWriter) hasher() {
	hasher := hasher.New()
	for b := range r.input {
		buf := bytes.NewBuffer(b.data)
		hasher.Reset()
		n, err := io.Copy(hasher, buf)
		if err != nil {
			r.setErr(err)
			return
		}
		if int(n) != len(b.data) {
			panic("short write")
		}
		_ = hasher.Sum(b.sha1Hash[:0])
		b.hashDone <- nil
	}
}

// writer will write hashed blocks to the output
// and recycle the buffers.
func (r *fixedWriter) writer() {
	defer close(r.exited)
	for b := range r.write {
		_ = <-b.hashDone
		match, ok := r.index[b.sha1Hash]
		if !ok {
			buf := bytes.NewBuffer(b.data)
			n, err := io.Copy(r.blks, buf)
			if err != nil {
				r.setErr(err)
				return
			}
			if int(n) != len(b.data) {
				panic("short write")
			}
			r.putUint64(0)
		} else {
			offset := b.N - match
			if offset <= 0 {
				panic("negative offset, should be impossible")
			}
			r.putUint64(uint64(offset))
		}
		// Update hash to latest match
		r.index[b.sha1Hash] = b.N

		// Done, reinsert buffer
		r.buffers <- b
	}
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

// Returns an approximate memory use in bytes for compression
// for the given number of blocks
func FixedMemUse(blocks int) int64 {
	bl := big.NewInt(int64(blocks))
	perBlock := big.NewInt(int64(HashSize + 8 /*int64*/ + 24 /* map entry*/))
	total := bl.Mul(bl, perBlock)
	if total.BitLen() > 63 {
		return math.MaxInt64
	}
	return total.Int64()
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
