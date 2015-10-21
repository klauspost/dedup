//+build ignore

// DISABLED, since I have found no scenarios where it provides improvement

package dedup

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"testing"
)

const (
	// Dynamic block size, including split on file signatures.
	// There are a number of typical file signartures builtin,
	// or you can use AddSignature to add your own.
	ModeDynamicSignatures = 2

	// Dynamic block size only split on file signatures
	ModeSignaturesOnly = 3
)

// Split on zpaq hash, file signatures and maximum block size.
func (z *zpaqWriter) writeFile(w *writer, b []byte) (int, error) {
	c1 := z.c1

	for i, c := range b {
		split := false
		v := sigmap[c]
		if len(v) > 0 && i < len(b)-6 {
			for _, s := range v {
				split = true
				for j, expect := range s {
					if b[j+1] != expect {
						split = false
						break
					}
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
				split = true
				for j, expect := range s {
					if b[j+1] != expect {
						split = false
						break
					}
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
		err := AddSignature(sig[1 : 1+l])
		if err != nil {
			panic(err)
		}
	}
}

// ErrSignatureTooShort is returned if AddSignature is called
// with a signature shorter than 3 bytes
var ErrSignatureTooShort = errors.New("signature should be at least 2 bytes")

// AddSignature will add a signature that will cause a block
// split. The signature must be more than 1 byte (at least 3 is recommended),
// and only up to 7 bytes are compared.
func AddSignature(b []byte) error {
	if len(b) <= 1 {
		return ErrSignatureTooShort
	}
	if len(b) > 7 {
		b = b[:7]
	}
	x := sigmap[b[0]]
	dst := make([]byte, len(b)-1)
	copy(dst, b[1:])
	x = append(x, dst)
	sigmap[b[0]] = x
	return nil
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

// Tests:

// Maximum block size: 64k
func BenchmarkDynamicSigsWriter64K(t *testing.B) {
	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 64 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 50; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeDynamicSignatures, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Maximum block size: 64k
func BenchmarkSigsOnlyWriter64K(t *testing.B) {
	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 64 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 50; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeSignaturesOnly, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
