package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"testing"
	"time"
)

const testfile = "10G.data"
const Mega = 1024 * 1024

func init() {
	go http.ListenAndServe(":6060", nil)
	f, err := os.Open(testfile)
	if err != nil {
		log.Println("generating", testfile)
		generate10G()
	} else {
		f.Close()
	}
}

type dummyReader struct {
	count int
	max   int
	rnd   *rand.Rand
}

func (dr *dummyReader) Read(p []byte) (n int, err error) {
	var alpha = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ             ")
	if dr.count == dr.max {
		return 0, io.EOF
	}

	remain := len(p)
	idx := 0
	for remain > 0 {
		p[idx] = alpha[dr.rnd.Intn(len(alpha))]
		idx++
		remain--
		dr.count++
		if dr.count == dr.max {
			return idx, io.EOF
		}
	}

	return idx, nil
}

func newDummyReader(cap int) *dummyReader {
	dr := new(dummyReader)
	dr.max = cap
	dr.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	return dr
}

func generate10G() {
	dr := newDummyReader(10 * 1024 * Mega)
	f, err := os.OpenFile(testfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}

	io.Copy(f, dr)

	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func TestReduce(t *testing.T) {
	reducer := new(uniqueReducer)
	reduce(22, reducer)
}

func TestFindUniqueString(t *testing.T) {
	t0 := bytes.NewBufferString("   ")
	findUnique(t0, 128*Mega)
	t1 := bytes.NewBufferString("a a b b c b") // 4
	findUnique(t1, 128*Mega)
	t2 := bytes.NewBufferString("a a a a a a") // no
	findUnique(t2, 128*Mega)
	t3 := bytes.NewBufferString("a b c d e a") //1
	findUnique(t3, 128*Mega)
	t4 := bytes.NewBufferString("a a a a a b") // 5
	findUnique(t4, 128*Mega)
}

func TestFindUnique100M(t *testing.T) {
	file, err := os.Open(testfile)
	if err != nil {
		log.Fatal(err)
	}
	findUnique(io.LimitReader(file, 100*Mega), 50*Mega)
}
func TestFindUnique1G(t *testing.T) {
	file, err := os.Open(testfile)
	if err != nil {
		log.Fatal(err)
	}
	findUnique(io.LimitReader(file, 1000*Mega), 500*Mega)
}

func TestFindUnique10G(t *testing.T) {
	file, err := os.Open(testfile)
	if err != nil {
		log.Fatal(err)
	}
	findUnique(io.LimitReader(file, 10000*Mega), 2000*Mega)
}
