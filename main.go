package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////
// pre-processing stage
// an in-memory sorter

// key-value construction
// rawEntry binary format:
// bytesSize int32
// bytesPtr int32
type rawEntry []byte

const entrySize = 8

func (e rawEntry) sz() int32  { return int32(binary.LittleEndian.Uint32(e[:])) }
func (e rawEntry) ptr() int32 { return int32(binary.LittleEndian.Uint32(e[4:])) }

// value binds to specific bytes buffer
func (e rawEntry) value(buf []byte) rawValue { return buf[e.ptr():][:e.sz()] }

// value binary format
// ord int64
// line []byte
type rawValue []byte

func (v rawValue) ord() int64    { return int64(binary.LittleEndian.Uint64(v[:])) }
func (v rawValue) bytes() []byte { return v[8:] }

type entry struct {
	bts []byte
	ord int64 // data order
}

// split large slice set into a group of small sets
// for limiting memory usage, we write content backwords
// and write idx forwards, as:
// [key0,key1,...keyN,..... valueN, .... value0]
type dataSet struct {
	buf         []byte
	idxWritten  int
	dataWritten int
	dataPtr     int // point to last non-writable place, dataPtr -1 will be writable

	idxPtr  int // point to next index place
	idxSize int
	swapbuf [entrySize]byte
}

func newDataSet(sz int) *dataSet {
	e := new(dataSet)
	e.buf = make([]byte, sz)
	e.dataPtr = sz
	return e
}

// Add bytes with it's data record order
func (s *dataSet) Add(bts []byte, ord int64) bool {
	sz := len(bts) + 8
	if s.idxWritten+s.dataWritten+sz+entrySize >= len(s.buf) {
		return false
	}

	// write data
	s.dataPtr -= sz
	s.dataWritten += sz
	binary.LittleEndian.PutUint64(s.buf[s.dataPtr:], uint64(ord))
	copy(s.buf[s.dataPtr+8:], bts)

	// write idx
	binary.LittleEndian.PutUint32(s.buf[s.idxPtr:], uint32(sz))
	binary.LittleEndian.PutUint32(s.buf[s.idxPtr+4:], uint32(s.dataPtr))
	s.idxPtr += entrySize
	s.idxWritten += entrySize

	return true
}

// return the ith entry in binary form
func (s *dataSet) e(i int) rawEntry {
	return rawEntry(s.buf[i*entrySize : i*entrySize+entrySize])
}

// return the ith element in object form
func (s *dataSet) get(i int) entry {
	v := s.e(i).value(s.buf)
	return entry{v.bytes(), v.ord()}
}

func (s *dataSet) Len() int { return s.idxWritten / entrySize }
func (s *dataSet) Less(i, j int) bool {
	v1 := s.e(i).value(s.buf)
	v2 := s.e(j).value(s.buf)
	return bytes.Compare(v1.bytes(), v2.bytes()) < 0
}

func (s *dataSet) Swap(i, j int) {
	copy(s.swapbuf[:], s.e(i))
	copy(s.e(i), s.e(j))
	copy(s.e(j), s.swapbuf[:])
}

// data set reader for heap aggregation
type dataSetReader struct {
	set  *dataSet
	head int
	elem entry
}

func newDataSetReader(set *dataSet) *dataSetReader {
	if set.Len() == 0 {
		return nil
	}
	esr := new(dataSetReader)
	esr.set = set
	esr.elem = set.get(0)
	return esr
}

func (esr *dataSetReader) next() bool {
	esr.head++
	if esr.head >= esr.set.Len() {
		return false
	}
	esr.elem = esr.set.get(esr.head)
	return true
}

// memory based aggregator
type memSortAggregator struct {
	sets []*dataSetReader
}

func (h *memSortAggregator) Len() int { return len(h.sets) }
func (h *memSortAggregator) Less(i, j int) bool {
	return bytes.Compare(h.sets[i].elem.bts, h.sets[j].elem.bts) < 0
}
func (h *memSortAggregator) Swap(i, j int)      { h.sets[i], h.sets[j] = h.sets[j], h.sets[i] }
func (h *memSortAggregator) Push(x interface{}) { h.sets = append(h.sets, x.(*dataSetReader)) }
func (h *memSortAggregator) Pop() interface{} {
	n := len(h.sets)
	x := h.sets[n-1]
	h.sets = h.sets[0 : n-1]
	return x
}

// memory bounded sorter for big data
type sorter struct {
	sets    []*dataSet
	setSize int
	limit   int // max total memory usage for sorting
}

func (h *sorter) Len() int {
	n := 0
	for k := range h.sets {
		n += h.sets[k].Len()
	}
	return n
}

func (h *sorter) Serialize(w io.Writer) {
	if len(h.sets) > 0 {
		agg := new(memSortAggregator)
		for k := range h.sets {
			log.Println("sorting sets#", k)
			sort.Sort(h.sets[k])
			heap.Push(agg, newDataSetReader(h.sets[k]))
		}
		log.Println("merging sorted sets to file")

		written := 0
		esr := heap.Pop(agg).(*dataSetReader)
		last := esr.elem
		last_cnt := 1
		if esr.next() {
			heap.Push(agg, esr)
		}

		for agg.Len() > 0 {
			esr = heap.Pop(agg).(*dataSetReader)
			elem := esr.elem
			if bytes.Compare(elem.bts, last.bts) == 0 { // condense output
				last_cnt++
			} else {
				// TODO : need to define formal output format
				fmt.Fprintf(w, "%v,%v,%v\n", string(last.bts), last.ord, last_cnt)
				last = elem
				last_cnt = 1
				written++
			}
			if esr.next() {
				heap.Push(agg, esr)
			}
		}
		fmt.Fprintf(w, "%v,%v,%v\n", string(last.bts), last.ord, last_cnt)
		written++
		log.Println("written", written, "elements")
		h.sets = nil
	}
}

// Add controls the memory for every input
func (h *sorter) Add(bts []byte, ord int64) bool {
	if h.sets == nil { // init first one
		h.sets = []*dataSet{newDataSet(h.setSize)}
	}

	set := h.sets[len(h.sets)-1]
	if !set.Add(bts, ord) {
		if h.setSize*(len(h.sets)+1) > h.limit { // limit reached
			return false
		}
		newSet := newDataSet(h.setSize)
		h.sets = append(h.sets, newSet)
		newSet.Add(bts, ord)
	}
	return true
}

func (h *sorter) init(limit int) {
	h.setSize = 1 << 24 // 16MB set
	h.limit = limit
	if h.limit < h.setSize {
		h.limit = h.setSize
	}
}

// sort2Disk writes strings with it's ordinal and count
// xxxxx,1234,1
// aaaa,5678,10
func sort2Disk(r io.Reader, memLimit int) int {
	h := new(sorter)
	h.init(memLimit)
	var ord int64
	parts := 0

	// file based serialization
	fileDump := func(hp *sorter, path string) {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			log.Fatal(err)
		}
		hp.Serialize(f)
		runtime.GC()
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}

	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		if !h.Add(scanner.Bytes(), ord) {
			fileDump(h, fmt.Sprintf("part%v.dat", parts))
			log.Println("chunk#", parts, "written")
			parts++
			h.Add(scanner.Bytes(), ord)
		}
		ord++

	}
	if err := scanner.Err(); err != nil {
		log.Fatal("error reading from source")
	}

	if h.Len() > 0 {
		fileDump(h, fmt.Sprintf("part%v.dat", parts))
		log.Println("chunk#", parts, "written")
		parts++
	}
	return parts
}

////////////////////////////////////////////////////////////////////////////////
// disk streaming stage
type streamReader struct {
	scanner *bufio.Scanner
	str     string // the head element
	ord     string
	cnt     string
}

func (sr *streamReader) next() bool {
	if sr.scanner.Scan() {
		strs := strings.Split(sr.scanner.Text(), ",")
		sr.str = strs[0]
		sr.ord = strs[1]
		sr.cnt = strs[2]
		return true
	}
	return false
}

func newStreamReader(r io.Reader) *streamReader {
	sr := new(streamReader)
	sr.scanner = bufio.NewScanner(r)
	sr.scanner.Split(bufio.ScanLines)
	return sr
}

// streamAggregator always pop the min string
type streamAggregator struct {
	entries []*streamReader
}

func (h *streamAggregator) Len() int           { return len(h.entries) }
func (h *streamAggregator) Less(i, j int) bool { return h.entries[i].str < h.entries[j].str }
func (h *streamAggregator) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *streamAggregator) Push(x interface{}) { h.entries = append(h.entries, x.(*streamReader)) }
func (h *streamAggregator) Pop() interface{} {
	n := len(h.entries)
	x := h.entries[n-1]
	h.entries = h.entries[0 : n-1]
	return x
}

type countedEntry struct {
	str string
	ord int64
	cnt int64
}

func merger(parts int) chan countedEntry {
	ch := make(chan countedEntry, 4096)
	go func() {
		files := make([]*os.File, parts)
		h := new(streamAggregator)
		for i := 0; i < parts; i++ {
			f, err := os.Open(fmt.Sprintf("part%v.dat", i))
			if err != nil {
				log.Fatal(err)
			}
			files[i] = f
			sr := newStreamReader(f)
			sr.next() // fetch first string,ord
			heap.Push(h, sr)
		}

		for h.Len() > 0 {
			sr := heap.Pop(h).(*streamReader)
			ord, _ := strconv.ParseInt(sr.ord, 10, 64)
			cnt, _ := strconv.ParseInt(sr.cnt, 10, 64)
			ch <- countedEntry{sr.str, ord, cnt}
			if sr.next() {
				heap.Push(h, sr)
			}
		}
		close(ch)

		for _, f := range files[:] {
			if err := f.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}()

	return ch
}

// findUnique reads from r with a specified bufsize
// and trys to find the first unique string in this file
func findUnique(r io.Reader, memLimit int) {
	// step.1 sort into file chunks
	parts := sort2Disk(r, memLimit)
	log.Println("generated", parts, "parts")
	// step2. sequential output of all parts
	ch := merger(parts)
	log.Println("beginning merged sequential output")

	// step3. loop through the sorted string chan
	// and find the unique string with lowest ord
	var target_str string
	var target_ord int64
	var hasSet bool

	var last_str string
	var last_ord int64
	var last_cnt int64
	if e, ok := <-ch; ok {
		last_str = e.str
		last_ord = e.ord
		last_cnt = e.cnt
	} else {
		log.Println("empty set")
		return
	}

	compareTarget := func() {
		if last_cnt == 1 {
			// found new unique string, compare with the ordinal
			if !hasSet {
				target_str = last_str
				target_ord = last_ord
				hasSet = true
			} else if last_ord < target_ord {
				target_str = last_str
				target_ord = last_ord
			}
		}
	}

	// read through the sorted string chan
	for e := range ch {
		if last_str == e.str {
			last_cnt += e.cnt
		} else {
			compareTarget()
			last_str = e.str
			last_ord = e.ord
			last_cnt = e.cnt
		}
	}

	// make sure the final words is considered
	compareTarget()

	if hasSet {
		log.Println("Found the first unique string:", string(target_str), "appears at:", target_ord)
	} else {
		log.Println("Unique string not found!")
	}
}
