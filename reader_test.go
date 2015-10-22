package dedup_test

import (
	"bytes"
	"io"
	"testing"

	"io/ioutil"

	"fmt"

	"github.com/klauspost/dedup"
)

func TestReader(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	const totalinput = 10<<20 + 65
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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Fixed Index size:", idx.Len())
	t.Log("Fixed Data size:", data.Len(), "-", data.Len()*100/totalinput, "%")

	r, err := dedup.NewReader(&idx, &data)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Maximum estimated memory:", r.MaxMem(), "bytes")

	out, err := ioutil.ReadAll(r)
	if err != io.EOF && err != nil {
		t.Fatal(err)
	}
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if bytes.Compare(b, out) != 0 {
		t.Fatal("Output mismatch")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	blocks := r.BlockSizes()
	for _, s := range blocks[:len(blocks)-1] {
		if s != size {
			t.Fatal("wrong size, expected", size, "got", s)
		}
	}
}

func TestReaderStream(t *testing.T) {
	data := bytes.Buffer{}

	const totalinput = 10<<20 + 65
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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewStreamWriter(&data, dedup.ModeFixed, size, 10*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Fixed Data size:", data.Len(), "-", data.Len()*100/totalinput, "%")

	r, err := dedup.NewStreamReader(&data)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Maximum estimated memory:", r.MaxMem(), "bytes")

	out, err := ioutil.ReadAll(r)
	if err != io.EOF && err != nil {
		t.Fatal(err)
	}
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if bytes.Compare(b, out) != 0 {
		t.Fatal("Output mismatch")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeekReader(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	const totalinput = 50<<20 + 65
	input := getBufferSize(totalinput)

	const size = 64 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 500; i++ {
		// Read from 10 first blocks
		src := b[(i%100)*size : (i%100)*size+size]
		// Write into the following ones
		dst := b[(100+i)*size : (i+100)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Fixed Index size:", idx.Len())
	t.Log("Fixed Data size:", data.Len(), "-", data.Len()*100/totalinput, "%")

	r, err := dedup.NewSeekReader(&idx, bytes.NewReader(data.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Maximum estimated memory:", r.MaxMem(), "bytes")

	out, err := ioutil.ReadAll(r)
	if err != io.EOF && err != nil {
		t.Fatal(err)
	}
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if bytes.Compare(b, out) != 0 {
		t.Fatal("Output mismatch")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDynamicRoundtrip(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	const totalinput = 10<<20 + 65
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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeDynamic, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Dynamic Index size:", idx.Len())
	t.Log("Dynamic Data size:", data.Len())

	r, err := dedup.NewReader(&idx, &data)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Maximum estimated memory:", r.MaxMem(), "bytes")
	blocks := r.BlockSizes()
	avg := 0
	for _, v := range blocks {
		if v > size {
			t.Fatal("too big block returned, should not be >", size, "was", v)
		}
		avg += v
	}
	t.Log("Average block size:", avg/len(blocks), "bytes")

	out, err := ioutil.ReadAll(r)
	if err != io.EOF && err != nil {
		t.Fatal(err)
	}
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if bytes.Compare(b, out) != 0 {
		t.Fatal("Output mismatch")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReaderWriteTo(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	const totalinput = 10<<20 + 65
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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	r, err := dedup.NewReader(&idx, &data)
	if err != nil {
		t.Fatal(err)
	}

	dst := &bytes.Buffer{}
	n, err := r.WriteTo(dst)
	if err != io.EOF && err != nil {
		t.Fatal(err)
	}
	if len(b) != int(n) {
		t.Errorf("Write count, expected n %d, got %d", len(b), n)
	}

	out := dst.Bytes()
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if len(b) != len(out) {
		t.Fatalf("Expected len %d, got %d", len(b), len(out))
	}
	if bytes.Compare(b, out) != 0 {
		t.Fatal("Output mismatch")
	}
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	blocks := r.BlockSizes()
	for _, s := range blocks[:len(blocks)-1] {
		if s != size {
			t.Fatal("wrong size, expected", size, "got", s)
		}
	}
}

// Indexed stream, 10MB input, 64K blocks
func BenchmarkReader64K(t *testing.B) {
	idx := &bytes.Buffer{}
	data := &bytes.Buffer{}

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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(idx, data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(w, input)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	index := idx.Bytes()
	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		idx = bytes.NewBuffer(index)
		data = bytes.NewBuffer(alldata)
		r, err := dedup.NewReader(idx, data)
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != int64(len(b)) {
			t.Fatal("read was short, expected", len(b), "was", n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Indexed stream, 10MB input, 4K blocks
func BenchmarkReader4K(t *testing.B) {
	idx := &bytes.Buffer{}
	data := &bytes.Buffer{}

	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 4 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 500; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(idx, data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(w, input)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	index := idx.Bytes()
	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		idx := bytes.NewBuffer(index)
		data := bytes.NewBuffer(alldata)
		r, err := dedup.NewReader(idx, data)
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != int64(len(b)) {
			t.Fatal("read was short, expected", len(b), "was", n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Indexed stream, 10MB input, 1K blocks
func BenchmarkReader1K(t *testing.B) {
	idx := &bytes.Buffer{}
	data := &bytes.Buffer{}

	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 1 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 500; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(idx, data, dedup.ModeFixed, size, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(w, input)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	index := idx.Bytes()
	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		idx := bytes.NewBuffer(index)
		data := bytes.NewBuffer(alldata)
		r, err := dedup.NewReader(idx, data)
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != int64(len(b)) {
			t.Fatal("read was short, expected", len(b), "was", n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Stream, 64K blocks on 10MB data.
func BenchmarkReaderStream64K(t *testing.B) {
	data := &bytes.Buffer{}

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
	input = bytes.NewBuffer(b)
	w, err := dedup.NewStreamWriter(data, dedup.ModeFixed, size, 100*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input := bytes.NewBuffer(alldata)
		r, err := dedup.NewStreamReader(input)
		if err != nil {
			t.Fatal(err)
		}

		n, err := io.Copy(ioutil.Discard, r)
		if err != io.EOF && err != nil {
			t.Fatal(err)
		}
		if len(b) != int(n) {
			t.Fatalf("Expected len %d, got %d", len(b), n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Stream, 4K blocks on 10MB data.
func BenchmarkReaderStream4K(t *testing.B) {
	data := &bytes.Buffer{}

	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 4 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 100; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewStreamWriter(data, dedup.ModeFixed, size, 100*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input := bytes.NewBuffer(alldata)
		r, err := dedup.NewStreamReader(input)
		if err != nil {
			t.Fatal(err)
		}

		n, err := io.Copy(ioutil.Discard, r)
		if err != io.EOF && err != nil {
			t.Fatal(err)
		}
		if len(b) != int(n) {
			t.Fatalf("Expected len %d, got %d", len(b), n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Stream, 1K blocks on 10MB data.
func BenchmarkReaderStream1K(t *testing.B) {
	data := &bytes.Buffer{}

	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 1 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 500; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+i)*size : (i+10)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewStreamWriter(data, dedup.ModeFixed, size, 100*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	alldata := data.Bytes()

	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input := bytes.NewBuffer(alldata)
		r, err := dedup.NewStreamReader(input)
		if err != nil {
			t.Fatal(err)
		}

		n, err := io.Copy(ioutil.Discard, r)
		if err != io.EOF && err != nil {
			t.Fatal(err)
		}
		if len(b) != int(n) {
			t.Fatalf("Expected len %d, got %d", len(b), n)
		}
		err = r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// This will deduplicate a buffer of zeros to an indexed stream
func ExampleNewReader() {
	// Create data we can read.
	var idx, data bytes.Buffer
	input := bytes.NewBuffer(make([]byte, 50000))
	w, _ := dedup.NewWriter(&idx, &data, dedup.ModeFixed, 1000, 0)
	_, _ = io.Copy(w, input)
	_ = w.Close()

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

	// OUTPUT: Memory use: 1000
	// Returned data length: 50000
	// Everything zero: true
}

// This will deduplicate a buffer of zeros to an indexed stream
func ExampleNewStreamReader() {
	// Create data we can read.
	var data bytes.Buffer
	input := bytes.NewBuffer(make([]byte, 50000))
	// Set the memory limit to 10000 bytes
	w, _ := dedup.NewStreamWriter(&data, dedup.ModeFixed, 1000, 10000)
	_, _ = io.Copy(w, input)
	_ = w.Close()

	r, err := dedup.NewStreamReader(&data)
	if err != nil {
		panic(err)
	}

	// Inspect how much memory it will use.
	// Since this is a stream, it will print the worst possible scenario
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

	// OUTPUT: Memory use: 10000
	// Returned data length: 50000
	// Everything zero: true
}
