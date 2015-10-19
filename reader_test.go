package dedup_test

import (
	"bytes"
	"io"
	"testing"

	"io/ioutil"

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
	w, err := dedup.NewStreamWriter(&data, dedup.ModeFixed, size, 10)
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

	const totalinput = 30<<20 + 65
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
	io.Copy(w, input)
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
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(b)) {
			t.Fatal("read was short")
		}
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

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
	io.Copy(w, input)
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
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(b)) {
			t.Fatal("read was short")
		}
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
