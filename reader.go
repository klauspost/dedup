package dedup

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

//
type fixedMemReader struct {
	blocks []*rblock
	in     io.Reader
	size   int
}

// rblock contains read information about a single block
type rblock struct {
	data     []byte
	readData uint64
	first    int // Index of first occurence
	last     int // Index of last occurence
}

func (r *rblock) String() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{Read:%d; [%d:%d]}", r.readData, r.first, r.last)
}

var ErrUnknownFormat = errors.New("unknown index format")

func NewReader(index io.Reader, blocks io.Reader) (io.ReadCloser, error) {
	f := &fixedMemReader{in: blocks}
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
	fmt.Println("%v", f.blocks)
	return f, err
}

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
			f.blocks = append(f.blocks, &rblock{first: i, last: i, readData: size})

		// Last block
		case math.MaxUint64:
			r, err := binary.ReadUvarint(idx)
			if err != nil {
				return err
			}
			f.blocks = append(f.blocks, &rblock{readData: r})
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
	return nil
}

func (f *fixedMemReader) Read(b []byte) (int, error) {
	return 0, nil
}

func (f *fixedMemReader) Close() error {
	return nil
}
