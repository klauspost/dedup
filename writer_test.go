package dedup_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/klauspost/dedup"
)

// Returns a deterministic buffer of size n
func getBufferSize(n int) *bytes.Buffer {
	rand.Seed(0)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(255))
	}
	return bytes.NewBuffer(b)
}

func TestFixedWriter(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

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
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, size, size*10)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	removed := ((totalinput) - data.Len()) / size

	t.Log(dedup.BirthdayProblem(totalinput / size))
	t.Log("Index size:", idx.Len())
	t.Log("Data size:", data.Len())
	t.Log("Removed", removed, "blocks")
	// We should get at least 50 blocks
	if removed < 50 {
		t.Fatal("didn't remove at least 50 blocks")
	}
	if removed > 60 {
		t.Fatal("removed unreasonable high amount of blocks")
	}
}

func TestFixedWriterLimit(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	const totalinput = 10 << 20
	const limit = 9
	input := getBufferSize(totalinput)

	const size = 64 << 10
	b := input.Bytes()
	// Create some duplicates
	for i := 0; i < 50; i++ {
		// Read from 10 first blocks
		src := b[(i%10)*size : (i%10)*size+size]
		// Write into the following ones
		dst := b[(10+50-i)*size : (10+50-i)*size+size]
		copy(dst, src)
	}
	input = bytes.NewBuffer(b)
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, size, limit*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	removed := ((totalinput) - data.Len()) / size

	t.Log("Index size:", idx.Len())
	t.Log("Data size:", data.Len())
	t.Log("Removed", removed, "blocks")
	// We should get at least 50 blocks
	if removed > 10 {
		t.Fatal("it did not appear to respect the limit")
	}
	if removed < 8 {
		t.Fatal("removed too many blocks")
	}
	r, err := dedup.NewReader(&idx, &data)
	if err != nil {
		t.Fatal(err)
	}

	useBlocks := r.MaxMem() / size
	if useBlocks > 9 {
		t.Fatal("Uses too much memory, expected", limit, "got", useBlocks)
	}
	t.Log("Maximum estimated use:", r.MaxMem(), "bytes,", useBlocks, "blocks")
	r.Close()
}

func TestFixedFragment(t *testing.T) {
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
	out := make(chan dedup.Fragment, 10)
	count := make(chan int, 0)
	go func() {
		n := 0
		for f := range out {
			n += len(f.Payload)
		}
		count <- n
	}()
	input = bytes.NewBuffer(b)
	w, err := dedup.NewSplitter(out, dedup.ModeFixed, size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	datalen := <-count
	removed := ((totalinput) - datalen) / size

	t.Log("Data size:", datalen)
	t.Log("Removed", removed, "blocks")
	// We should get at least 50 blocks
	if removed < 50 {
		t.Fatal("didn't remove at least 50 blocks")
	}
	if removed > 60 {
		t.Fatal("removed unreasonable high amount of blocks")
	}
}

func TestDynamicWriter(t *testing.T) {
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

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
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeDynamic, size, 10*8*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	removed := ((totalinput) - data.Len()) / size

	t.Log("Dynamic Index size:", idx.Len())
	t.Log("Dynamic Data size:", data.Len())
	t.Log("Removed", removed, "blocks")
	// We don't know how many, but it should remove some blocks
	if removed < 40 {
		t.Fatal("didn't remove at least 40 blocks")
	}
}

func TestFixedStreamWriter(t *testing.T) {
	data := bytes.Buffer{}

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
	w, err := dedup.NewStreamWriter(&data, dedup.ModeFixed, size, 10*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	removed := ((totalinput) - data.Len()) / size

	t.Log("Data size:", data.Len())
	t.Log("Removed", removed, "blocks")
	// We should get at least 50 blocks, but there is a little overhead
	if removed < 49 {
		t.Fatal("didn't remove at least 49 blocks")
	}
	if removed > 60 {
		t.Fatal("removed unreasonable high amount of blocks")
	}
}

func TestDynamicStreamWriter(t *testing.T) {
	data := bytes.Buffer{}

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
	w, err := dedup.NewStreamWriter(&data, dedup.ModeDynamic, size, 10*8*size)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(w, input)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	removed := ((totalinput) - data.Len()) / size

	t.Log("Dynamic Data size:", data.Len())
	t.Log("Removed", removed, "blocks")
	// We don't know how many, but it should remove some blocks
	if removed < 40 {
		t.Fatal("didn't remove at least 40 blocks")
	}
}

func BenchmarkFixedWriter64K(t *testing.B) {
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
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkFixedWriter4K(t *testing.B) {
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
	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkFixedWriter1K(t *testing.B) {
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
	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Maximum block size:64k
func BenchmarkDynamicWriter64K(t *testing.B) {
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
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeDynamic, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Maximum block size:64k
func BenchmarkDynamicFragments64K(t *testing.B) {
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
		out := make(chan dedup.Fragment, 10)
		go func() {
			for _ = range out {
			}
		}()
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewSplitter(out, dedup.ModeDynamic, size)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Maximum block size:4k
func BenchmarkDynamicWriter4K(t *testing.B) {
	const totalinput = 10 << 20
	input := getBufferSize(totalinput)

	const size = 4 << 10
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
		w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeDynamic, size, 0)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkFixedStreamWriter4K(t *testing.B) {
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
	t.ResetTimer()
	t.SetBytes(totalinput)
	for i := 0; i < t.N; i++ {
		input = bytes.NewBuffer(b)
		w, _ := dedup.NewStreamWriter(ioutil.Discard, dedup.ModeFixed, size, 10*size)
		io.Copy(w, input)
		err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// This doesn't actually test anything, but prints probabilities to log
func TestBirthdayProblem(t *testing.T) {
	t.Log("Hash size is", dedup.HashSize*8, "bits")
	t.Log("1GiB, 1KiB blocks:")
	t.Log(dedup.BirthdayProblem((1 << 30) / (1 << 10)))
	w, _ := dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, 1<<10, 0)
	e, _ := w.MemUse(1 << 30)
	t.Logf("It will use %d MiB for encoder.", e>>20)

	t.Log("1TiB, 4KiB blocks:")
	t.Log(dedup.BirthdayProblem((1 << 40) / (4 << 10)))
	w, _ = dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, 4<<10, 0)
	e, _ = w.MemUse(1 << 40)
	t.Logf("It will use %d MiB for encoder.", e>>20)

	t.Log("1PiB, 4KiB blocks:")
	t.Log(dedup.BirthdayProblem((1 << 50) / (4 << 10)))
	e, _ = w.MemUse(1 << 50)
	t.Logf("It will use %d MiB for encoder.", e>>20)

	t.Log("1EiB, 64KiB blocks:")
	t.Log(dedup.BirthdayProblem((1 << 60) / (64 << 10)))
	w, _ = dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, 64<<10, 0)
	e, _ = w.MemUse(1 << 60)
	t.Logf("It will use %d MiB for encoder.", e>>20)

	t.Log("1EiB, 1KiB blocks:")
	t.Log(dedup.BirthdayProblem((1 << 60) / (1 << 10)))
	w, _ = dedup.NewWriter(ioutil.Discard, ioutil.Discard, dedup.ModeFixed, 1<<10, 0)
	e, _ = w.MemUse(1 << 60)
	t.Logf("It will use %d MiB for encoder.", e>>20)
}

// This will deduplicate a buffer of zeros to an indexed stream
func ExampleNewWriter() {
	// We will write to these
	idx := bytes.Buffer{}
	data := bytes.Buffer{}

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 50000))

	// Create a new writer, with each block being 1000 bytes
	w, err := dedup.NewWriter(&idx, &data, dedup.ModeFixed, 1000, 0)
	if err != nil {
		panic(err)
	}

	// Copy our input to the writer.
	io.Copy(w, input)

	// Close the writer
	err = w.Close()
	if err != nil {
		panic(err)
	}

	// Let us inspect what was written:
	fmt.Println("Blocks:", w.Blocks())
	fmt.Println("Index size:", idx.Len())
	fmt.Println("Data size:", data.Len())

	// OUTPUT: Blocks: 50
	// Index size: 67
	// Data size: 1000
}

// This will deduplicate a buffer of zeros to an non-indexed stream
func ExampleNewStreamWriter() {
	// We will write to this
	data := bytes.Buffer{}

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 50000))

	// Create a new writer, with each block being 1000 bytes,
	// And allow it to use 10000 bytes of memory
	w, err := dedup.NewStreamWriter(&data, dedup.ModeFixed, 1000, 10000)
	if err != nil {
		panic(err)
	}
	// Copy our input to the writer.
	io.Copy(w, input)

	// Close the writer
	err = w.Close()
	if err != nil {
		panic(err)
	}

	// Let us inspect what was written:
	fmt.Println("Blocks:", w.Blocks())
	fmt.Println("Data size:", data.Len())

	// OUTPUT: Blocks: 50
	// Data size: 1068
}

// This will deduplicate a buffer of zeros,
// and return each block on a channel in order.
func ExampleNewSplitter() {
	// We will write to this
	// We set a small buffer
	out := make(chan dedup.Fragment, 10)

	// This will consume our blocks as they are returned
	// and send information about what was received.
	info := make(chan int, 0)
	go func() {
		n := 0
		size := 0
		for f := range out {
			n++
			size += len(f.Payload)
		}
		info <- n
		info <- size
	}()

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 50000))

	// Create a new writer, with each block being 1000 bytes,
	w, err := dedup.NewSplitter(out, dedup.ModeFixed, 1000)
	if err != nil {
		panic(err)
	}
	// Copy our input to the writer.
	io.Copy(w, input)

	// Close the writer
	err = w.Close()
	if err != nil {
		panic(err)
	}

	// Let us inspect what was written:
	fmt.Println("Blocks:", <-info)
	fmt.Println("Data size:", <-info)

	// OUTPUT: Blocks: 50
	// Data size: 1000
}

// This will deduplicate a file
// and return each block on a channel in order.
func ExampleNewSplitter_file() {
	// Our input
	f, _ := os.Open("LICENSE")
	defer f.Close()

	// We will receive fragments on this channel
	ch := make(chan dedup.Fragment, 10)

	var wg sync.WaitGroup
	wg.Add(1)

	// Start a goroutine that will consume the fragments
	go func() {
		defer wg.Done()
		for {
			select {
			case f, ok := <-ch:
				if !ok {
					return
				}
				fmt.Printf("Got fragment #%d, size %d, hash:%s, new:%t\n", f.N, len(f.Payload), hex.EncodeToString(f.Hash[:]), f.New)
				if f.New {
					// Insert payload into data store
				}
				// Add hash to list of hashes required to reconstruct the file.
			}
		}
	}()

	// Create a dynamic splitter with average size of 256 bytes.
	w, _ := dedup.NewSplitter(ch, dedup.ModeDynamic, 4*256)

	// Copy data to the splitter
	_, _ = io.Copy(w, f)

	// Flush the remaining fragments
	_ = w.Close()

	// Wait for input to be received.
	wg.Wait()
	// OUTPUT:
	// Got fragment #0, size 63, hash:e94cca23fbbec2018d254f653d610f33e5cf6991, new:true
	// Got fragment #1, size 174, hash:2aa31f8fce1f2b2b39fe15306b9f9d63c19a3857, new:true
	// Got fragment #2, size 364, hash:d47786c5918a1aa13d9a07768a18cb2aade6261d, new:true
	// Got fragment #3, size 100, hash:8d7885317178fc20956f6f66e45dd7a1c5d4b931, new:true
	// Got fragment #4, size 213, hash:0165c7b3e663366b15f53dcbec284f749f799c9f, new:true
}

// This example will show how to write data to two files.
// Running this example will deduplicate an empty byte slice
// of 500000 bytes into an 'output.data' and 'output.idx' file.
//
// In the real world, you would likely want to add a bufio.NewWriter
// to the output, but to keep it simple, we don't do that here.
func ExampleNewWriter_file() {
	data, err := os.Create("output.data")
	if err != nil {
		panic(err)
	}
	// Close, print stats and remove it
	defer func() {
		data.Close()
		stat, _ := os.Stat("output.data")
		fmt.Println("Data size:", stat.Size())
		os.Remove("output.data")
	}()

	idx, err := os.Create("output.idx")
	if err != nil {
		panic(err)
	}
	// Close, print stats and remove it
	defer func() {
		idx.Close()
		stat, _ := os.Stat("output.idx")
		fmt.Println("Index size:", stat.Size())
		os.Remove("output.idx")
	}()

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 500000))

	// Create a new writer, with each block being 1000 bytes fixed size.
	w, err := dedup.NewWriter(idx, data, dedup.ModeFixed, 1000, 0)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// Copy our input to the writer.
	io.Copy(w, input)

	// Print the number of blocks written
	fmt.Println("Blocks:", w.Blocks())

	// OUTPUT: Blocks: 500
	// Index size: 517
	// Data size: 1000
}

// This will deduplicate a buffer of zeros to an non-indexed stream
// written to a file.
// It is not recommended to use a single stream when you are writing to
// a stream.
func ExampleNewStreamWriter_file() {
	// We will write to this
	data, err := os.Create("outputstream.data")
	if err != nil {
		panic(err)
	}
	// Close, print stats and remove it
	defer func() {
		data.Close()
		stat, _ := os.Stat("outputstream.data")
		fmt.Println("Stream size:", stat.Size())
		os.Remove("outputstream.data")
	}()

	// This is our input:
	input := bytes.NewBuffer(make([]byte, 500000))

	// Create a new writer, with each block being 1000 bytes,
	// And allow it to use 10000 bytes of memory
	w, err := dedup.NewStreamWriter(data, dedup.ModeFixed, 1000, 10000)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// Copy our input to the writer.
	io.Copy(w, input)

	// Print the number of blocks written
	fmt.Println("Blocks:", w.Blocks())

	// OUTPUT: Blocks: 500
	// Stream size: 1518
}

// This shows an example of a birthday problem calculation.
// We calculate the probability of a collision of SHA1 hashes
// on 1 Terabyte data, using 1 Kilobyte blocks.
// With SHA-1, that gives a 1 in 2535301202817642046627252275200 chance
// of a collision occurring.
func ExampleBirthdayProblem() {
	fmt.Println("Hash size is", dedup.HashSize*8, "bits")
	fmt.Println("1TiB, 1KiB blocks:")
	fmt.Println(dedup.BirthdayProblem((1 << 40) / (1 << 10)))
	// Output: Hash size is 160 bits
	// 1TiB, 1KiB blocks:
	// Collision probability is ~ 1/2535301202817642046627252275200 ~ 3.944304522431639e-31
}
