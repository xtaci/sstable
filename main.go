package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
// pre-processing stage
// an in-memory sorter

// key-value construction
// rawEntry binary format:
// bytesSize uint32
// bytesPtr uint32
type rawEntry []byte

const entrySize = 8

func (e rawEntry) sz() uint32  { return binary.LittleEndian.Uint32(e[:]) }
func (e rawEntry) ptr() uint32 { return binary.LittleEndian.Uint32(e[4:]) }

// value binds to specific bytes buffer
func (e rawEntry) value(buf []byte) rawValue { return buf[e.ptr():][:e.sz()] }

// value binary format
// ord uint64
// line []byte
type rawValue []byte

func (v rawValue) ord() uint64   { return binary.LittleEndian.Uint64(v[:]) }
func (v rawValue) bytes() []byte { return v[8:] }

type entry struct {
	bts []byte
	ord uint64 // data order
}

// split large slice set into a group of small sets
// for limiting memory usage, we write content backwords
// and write idx forwards, as:
// [key0,key1,...keyN,..... valueN, .... value0]
type dataSet struct {
	buf []byte

	idxWritten  int
	dataWritten int
	dataPtr     int // point to last non-writable place, dataPtr -1 will be writable
	idxPtr      int // point to next index place

	swapbuf [entrySize]byte
}

func newDataSet(sz int) *dataSet {
	e := new(dataSet)
	e.buf = make([]byte, sz)
	e.dataPtr = sz
	return e
}

// Add bytes with it's data record order
func (s *dataSet) Add(bts []byte, ord uint64) bool {
	sz := len(bts) + 8
	if s.idxWritten+s.dataWritten+sz+entrySize >= len(s.buf) {
		return false
	}

	// write data
	s.dataPtr -= sz
	s.dataWritten += sz
	binary.LittleEndian.PutUint64(s.buf[s.dataPtr:], ord)
	copy(s.buf[s.dataPtr+8:], bts)

	// write idx
	binary.LittleEndian.PutUint32(s.buf[s.idxPtr:], uint32(sz))
	binary.LittleEndian.PutUint32(s.buf[s.idxPtr+4:], uint32(s.dataPtr))
	s.idxPtr += entrySize
	s.idxWritten += entrySize

	return true
}

// Reset to initial state for future use
func (s *dataSet) Reset() {
	s.dataPtr = len(s.buf)
	s.dataWritten = 0
	s.idxPtr = 0
	s.idxWritten = 0
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
	unused  []*dataSet
	setSize int
	limit   int // max total memory usage for sorting
}

// a mapper defines a mapping for `entry` to  bytes
type Mapper interface {
	Map(entry) []byte
	End() []byte
}

func (h *sorter) Len() int {
	n := 0
	for k := range h.sets {
		n += h.sets[k].Len()
	}
	return n
}

func (h *sorter) Map(w io.Writer, mapper Mapper) {
	if len(h.sets) > 0 {
		// sort the sets in parallel
		wg := new(sync.WaitGroup)
		for k := range h.sets {
			log.Println("sorting sets#", k, "element count:", h.sets[k].Len())
			wg.Add(1)
			go func() {
				sort.Sort(h.sets[k])
				wg.Done()
			}()
		}
		wg.Wait()

		log.Println("merging sorted sets to file")
		agg := new(memSortAggregator)
		for k := range h.sets {
			heap.Push(agg, newDataSetReader(h.sets[k]))
		}

		written := 0
		for agg.Len() > 0 {
			esr := heap.Pop(agg).(*dataSetReader)
			r := mapper.Map(esr.elem)
			if r != nil {
				w.Write(r)
				written++
			}
			if esr.next() {
				heap.Push(agg, esr)
			}
		}
		if r := mapper.End(); r != nil {
			w.Write(r)
			written++
		}

		log.Println("written", written, "elements")
		for k := range h.sets {
			h.sets[k].Reset()
			fmt.Println(h.sets[k])
		}
		h.unused = h.sets
		h.sets = nil
	}
}

func (h *sorter) allocateNewSet() *dataSet {
	var newSet *dataSet
	if len(h.unused) > 0 {
		sz := len(h.unused)
		newSet = h.unused[sz-1]
		h.unused = h.unused[:sz-1]
	} else {
		newSet = newDataSet(h.setSize)
	}
	h.sets = append(h.sets, newSet)
	return newSet
}

// Add controls the memory for every input
func (h *sorter) Add(bts []byte, ord uint64) bool {
	if len(h.sets) == 0 {
		h.allocateNewSet()
	}
	set := h.sets[len(h.sets)-1]
	if !set.Add(bts, ord) {
		if h.setSize*(len(h.sets)+1) > h.limit { // limit reached
			return false
		}
		newSet := h.allocateNewSet()
		newSet.Add(bts, ord)
	}
	return true
}

func (h *sorter) init(limit int) {
	h.limit = limit
	h.setSize = limit / runtime.NumCPU()

	// make sure one set is not larger than MaxUint32
	if h.setSize >= math.MaxUint32 {
		h.setSize = math.MaxUint32
	}
}

// sort2Disk sorts and maps the input and output to multiple
// sorted files
func sort2Disk(r io.Reader, memLimit int, mapper Mapper) int {
	h := new(sorter)
	h.init(memLimit)
	var ord uint64
	parts := 0

	// file based serialization
	fileDump := func(hp *sorter, path string) {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			log.Fatal(err)
		}
		bufw := bufio.NewWriterSize(f, 1<<20)
		hp.Map(bufw, mapper)
		bufw.Flush()
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
	r   io.Reader
	str string // the head element
	ord uint64
	cnt uint64
	buf bytes.Buffer
}

func (sr *streamReader) next() bool {
	sr.buf.Reset()
	_, err := io.CopyN(&sr.buf, sr.r, 4)
	if err != nil {
		return false
	}
	sz := binary.LittleEndian.Uint32(sr.buf.Bytes())
	sr.buf.Reset()
	_, err = io.CopyN(&sr.buf, sr.r, int64(sz))
	if err != nil {
		return false
	}

	bts := sr.buf.Bytes()
	sr.ord = binary.LittleEndian.Uint64(bts)
	sr.cnt = binary.LittleEndian.Uint64(bts[8:])
	sr.str = string(bts[16:])
	return true
}

func newStreamReader(r io.Reader) *streamReader {
	sr := new(streamReader)
	sr.r = bufio.NewReader(r)
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
	ord uint64
	cnt uint64
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
			ch <- countedEntry{sr.str, sr.ord, sr.cnt}
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

// define a mapping function for counting
type countMapper struct {
	last    entry
	lastCnt uint64
	hasLast bool
	buf     []byte
}

func (m *countMapper) prepareBuffer(sz int) {
	if cap(m.buf) < sz {
		m.buf = make([]byte, sz)
	} else {
		m.buf = m.buf[:sz]
	}
}

func (m *countMapper) writeLast() {
	// output format
	// size - 32bit
	// ord 64bit
	// cnt 64bit
	// bts (size - 16)
	sz := len(m.last.bts) + 8 + 8
	m.prepareBuffer(sz + 4)
	binary.LittleEndian.PutUint32(m.buf, uint32(sz))
	binary.LittleEndian.PutUint64(m.buf[4:], m.last.ord)
	binary.LittleEndian.PutUint64(m.buf[12:], m.lastCnt)
	copy(m.buf[20:], m.last.bts)
}

func (m *countMapper) Map(e entry) (ret []byte) {
	if !m.hasLast {
		m.lastCnt = 1
		m.hasLast = true
		m.last = e
		return nil
	}

	if bytes.Compare(e.bts, m.last.bts) == 0 { // counting
		m.lastCnt++
	} else {
		m.writeLast()
		m.last = e
		m.lastCnt = 1
		return m.buf
	}
	return nil
}

func (m *countMapper) End() (ret []byte) {
	if !m.hasLast {
		return nil
	}
	m.writeLast()
	return m.buf
}

// Reducer interface
type Reducer interface {
	Reduce(countedEntry)
	End()
}

type uniqueReducer struct {
	target    countedEntry
	last      countedEntry
	hasUnique bool
}

func (r *uniqueReducer) check() {
	if r.last.cnt == 1 {
		if !r.hasUnique {
			r.target = r.last
			r.hasUnique = true
		} else if r.last.ord < r.target.ord {
			r.target = r.last
		}
	}
}

func (r *uniqueReducer) Reduce(e countedEntry) {
	if r.last.str == e.str {
		r.last.cnt += e.cnt
	} else {
		r.check()
		r.last = e
	}
}

func (r *uniqueReducer) End() {
	r.check()
}

// findUnique reads from r with a specified bufsize
// and trys to find the first unique string in this file
func findUnique(r io.Reader, memLimit int) {
	// step.1 sort into file chunks, mapping stage
	parts := sort2Disk(r, memLimit, new(countMapper))
	log.Println("generated", parts, "parts")
	// step2. sequential output of all parts
	ch := merger(parts)
	log.Println("beginning merged sequential output")

	// step3. loop through the sorted string chan
	// and find the unique string with lowest ord
	// reducing stage
	reducer := new(uniqueReducer)
	if e, ok := <-ch; ok {
		reducer.Reduce(e)
	} else {
		log.Println("empty set")
		return
	}
	for e := range ch {
		reducer.Reduce(e)
	}
	reducer.End()

	if reducer.hasUnique {
		log.Println("Found the first unique element:", reducer.target)
	} else {
		log.Println("Unique element not found!")
	}
}
