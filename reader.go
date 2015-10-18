package dedup

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// A reader will decode a deduplicated stream and
// return the data as it was encoded.
// Use Close when done to release resources.
type Reader interface {
	io.ReadCloser

	// MaxMem returns the *maximum* memory required to decode the stream.
	MaxMem() int
}

//
type fixedMemReader struct {
	blocks       []*rblock
	in           io.Reader
	stream       *bufio.Reader
	size         int
	maxLength    uint64 // Maxmimum backreference count
	curBlock     int
	curData      []byte
	ready        chan *rblock
	closeReader  chan struct{}
	readerClosed chan struct{}
}

// rblock contains read information about a single block
type rblock struct {
	data     []byte
	readData int
	first    int   // Index of first occurrence
	last     int   // Index of last occurrence
	err      error // Read error?
}

func (r *rblock) String() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{Read:%d; [%d:%d]}", r.readData, r.first, r.last)
}

var ErrUnknownFormat = errors.New("unknown index format")

// NewReader returns a reader that will decode the supplied index and data stream.
//
// This is compatible content from the NewWriter function.
//
// When you are done with the Reader, use Close to release resources.
func NewReader(index io.Reader, blocks io.Reader) (Reader, error) {
	f := &fixedMemReader{
		in:           blocks,
		ready:        make(chan *rblock, 8), // Read up to 8 blocks ahead
		closeReader:  make(chan struct{}, 0),
		readerClosed: make(chan struct{}, 0),
		curBlock:     0,
	}
	idx := bufio.NewReader(index)
	format, err := binary.ReadUvarint(idx)
	if err != nil {
		return nil, err
	}

	switch format {
	case 1:
		err = f.readFormat1(idx)
	default:
		err = ErrUnknownFormat
	}
	go f.blockReader()

	//fmt.Println(f.blocks)
	return f, err
}

// NewStreamReader returns a reader that will decode the supplied data stream.
//
// This is compatible content from the NewStreamWriter function.
//
// When you are done with the Reader, use Close to release resources.
func NewStreamReader(in io.Reader) (Reader, error) {
	f := &fixedMemReader{
		ready:        make(chan *rblock, 8), // Read up to 8 blocks ahead
		closeReader:  make(chan struct{}, 0),
		readerClosed: make(chan struct{}, 0),
		curBlock:     0,
	}
	br := bufio.NewReader(in)
	format, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}

	switch format {
	case 2:
		err = f.readFormat2(br)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnknownFormat
	}

	f.stream = br
	go f.streamReader()

	return f, nil
}

// NewSeekRead returns a reader that will decode the supplied index and data stream.
//
// This is compatible content from the NewWriter function.
//
// No blocks will be kept in memory, but the block data input must be seekable.
//
// When you are done with the Reader, use Close to release resources.
func NewSeekReader(index io.Reader, blocks io.ReadSeeker) (Reader, error) {
	f := &fixedMemReader{
		in:           blocks,
		ready:        make(chan *rblock, 8), // Read up to 8 blocks ahead
		closeReader:  make(chan struct{}, 0),
		readerClosed: make(chan struct{}, 0),
		curBlock:     0,
	}
	idx := bufio.NewReader(index)
	format, err := binary.ReadUvarint(idx)
	if err != nil {
		return nil, err
	}

	switch format {
	case 1:
		err = f.readFormat1(idx)
	default:
		err = ErrUnknownFormat
	}
	go f.blockReader()

	//fmt.Println(f.blocks)
	return f, err
}

// NewStreamReader returns a reader that will decode the supplied data stream.
//
// This is compatible content from the NewStreamWriter function.
//
// No blocks will be kept in memory, but the block data input must be seekable.
//
// When you are done with the Reader, use Close to release resources.
func NewSeekStreamReader(in io.ReadSeeker) (Reader, error) {
	f := &fixedMemReader{
		ready:        make(chan *rblock, 8), // Read up to 8 blocks ahead
		closeReader:  make(chan struct{}, 0),
		readerClosed: make(chan struct{}, 0),
		curBlock:     0,
	}
	br := bufio.NewReader(in)
	format, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, err
	}

	switch format {
	case 2:
		err = f.readFormat2(br)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnknownFormat
	}

	f.stream = br
	go f.streamReader()

	return f, nil
}

// readFormat1 will read the index of format 1
// and prepare decoding
func (f *fixedMemReader) readFormat1(idx io.ByteReader) error {
	size, err := binary.ReadUvarint(idx)
	if err != nil {
		return err
	}
	f.size = int(size)

	// Insert empty block 0
	f.blocks = append(f.blocks, nil)
	i := 0
	// Read blocks
	for {
		i++
		offset, err := binary.ReadUvarint(idx)
		if err != nil {
			return err
		}
		switch offset {
		// new block
		case 0:
			r, err := binary.ReadUvarint(idx)
			if err != nil {
				return err
			}
			f.blocks = append(f.blocks, &rblock{first: i, last: i, readData: int(size - r)})

		// Last block
		case math.MaxUint64:
			r, err := binary.ReadUvarint(idx)
			if err != nil {
				return err
			}
			f.blocks = append(f.blocks, &rblock{readData: int(size - r)})
			return nil
		// Deduplicated block
		default:
			pos := len(f.blocks) - int(offset)
			if pos <= 0 || pos >= len(f.blocks) {
				return fmt.Errorf("invalid offset encountered at block %d, offset was %d", len(f.blocks), offset)
			}
			// Update last position.
			org := f.blocks[pos]
			org.last = i
			f.blocks = append(f.blocks, org)
		}
	}
}

// readFormat2 will read the header data of format 2
// and stop at the first block.
func (f *fixedMemReader) readFormat2(rd io.ByteReader) error {
	size, err := binary.ReadUvarint(rd)
	if err != nil {
		return err
	}
	if size < MinBlockSize {
		return ErrSizeTooSmall
	}
	f.size = int(size)

	maxLength, err := binary.ReadUvarint(rd)
	if err != nil {
		return err
	}
	if maxLength < 1 {
		return ErrMaxBlocksTooSmall
	}
	f.maxLength = maxLength
	return nil
}

// Read will read from the input stream and return the
// deduplicated data.
func (f *fixedMemReader) Read(b []byte) (int, error) {
	read := 0
	for len(b) > 0 {
		// Read next
		if len(f.curData) == 0 {
			f.curBlock++
			next, ok := <-f.ready
			if !ok {
				return read, io.EOF
			}
			if next.err != nil {
				return read, next.err
			}
			f.curData = next.data
			// We don't want to keep it, if this is the last block
			if f.curBlock == next.last {
				next.data = nil
			}
			if len(f.curData) == 0 {
				continue
			}
		}
		n := copy(b, f.curData)
		read += n
		b = b[n:]
		f.curData = f.curData[n:]
	}
	return read, nil
}

// MaxMem returns the estimated maximum RAM usage needed to
// unpack this content.
func (f *fixedMemReader) MaxMem() int {
	if f.maxLength > 0 {
		return int(f.maxLength) * f.size
	}
	i := 1 // Current block
	curUse := 0
	maxUse := 0
	for {
		b := f.blocks[i]
		if b.first == i {
			curUse += b.readData
		}
		if curUse > maxUse {
			maxUse = curUse
		}

		if b.last == i {
			curUse -= b.readData
		}

		i++
		// We read them all
		if i == len(f.blocks) {
			break
		}
	}
	return maxUse
}

// blockReader will read format 1 blocks and deliver them
// to the ready channel.
// The function will return if the stream is finished,
// or an error occurs
func (f *fixedMemReader) blockReader() {
	defer close(f.readerClosed)
	defer close(f.ready)

	i := 1 // Current block
	totalRead := 0
	for {
		b := f.blocks[i]
		// Read it?
		if len(b.data) != b.readData {
			b.data = make([]byte, b.readData)
			n, err := io.ReadFull(f.in, b.data)
			if err != nil {
				b.err = err
			} else if n != b.readData {
				b.err = io.ErrUnexpectedEOF
			}
			totalRead += n
		}
		// Send or close
		select {
		case <-f.closeReader:
			return
		case f.ready <- b:
		}
		// Exit because of an error
		if b.err != nil {
			return
		}
		i++
		// We read them all
		if i == len(f.blocks) {
			return
		}
	}
}

// streamReader will read blocks from a single stream
// and deliver them to the "ready" channel.
// The function will return if an error occurs or
// the stream is finished.
func (f *fixedMemReader) streamReader() {
	defer close(f.readerClosed)
	defer close(f.ready)

	totalRead := 0

	// Create backreference buffers
	blocks := make([][]byte, f.maxLength)
	for i := range blocks {
		blocks[i] = make([]byte, f.size)
	}

	i := uint64(1) // Current block
	for {
		b := &rblock{}
		lastBlock := false

		b.err = func() error {
			offset, err := binary.ReadUvarint(f.stream)
			if err != nil {
				return err
			}
			// Read it?
			if offset == 0 || offset == math.MaxUint64 {
				s, err := binary.ReadUvarint(f.stream)
				if err != nil {
					return err
				}
				size := f.size - int(s)
				if size > f.size || size <= 0 {
					return fmt.Errorf("invalid size encountered at block %d, size was %d", i, size)
				}
				b.data = make([]byte, size)
				n, err := io.ReadFull(f.stream, b.data)
				if err != nil {
					return err
				} else if n != len(b.data) {
					return io.ErrUnexpectedEOF
				}
				totalRead += n
				if offset == math.MaxUint64 {
					lastBlock = true
				}
			} else {
				if offset > f.maxLength {
					return fmt.Errorf("invalid offset encountered at block %d, offset was %d", i, offset)
				}
				pos := i - offset
				if pos <= 0 {
					return fmt.Errorf("invalid offset encountered at block %d, offset was %d", i, offset)
				}
				src := blocks[pos%f.maxLength]
				b.data = src
			}

			blocks[i%f.maxLength] = b.data
			return nil
		}()
		// Send or close
		select {
		case <-f.closeReader:
			return
		case f.ready <- b:
		}
		// Exit because of an error
		if b.err != nil || lastBlock {
			return
		}
		i++
	}
}

// Close the reader and shut down the running goroutines.
func (f *fixedMemReader) Close() error {
	select {
	case <-f.readerClosed:
	case f.closeReader <- struct{}{}:
		<-f.readerClosed
	}
	return nil
}
