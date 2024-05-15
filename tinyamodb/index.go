package tinyamodb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

const (
	keyWidth uint64 = 64 // SHA-256
	posWidth uint64 = 8
	entwidth        = keyWidth + posWidth
)

type index struct {
	file      *os.File
	buf       *bufio.Writer
	mu        sync.Mutex
	mmap      map[string]uint64   // [sha-256]uint64(storepos)
	dmap      map[string][]uint64 // duplication
	size      uint64
	latestKey string
}

func newIndex(f *os.File) (*index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx := &index{
		file: f,
		buf:  bufio.NewWriter(f),
		size: uint64(fi.Size()),
	}
	if err := idx.setup(); err != nil && err != io.EOF {
		return nil, err
	}
	return idx, nil
}

func (i *index) Read(in string) (pos uint64, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size == 0 {
		return 0, io.EOF
	}
	if in == "" {
		// latest
		in = i.latestKey
	}
	poss, err := i.readAll(in)
	if err != nil {
		return 0, err
	}
	return poss[len(poss)-1], nil
}

func (i *index) Write(in string, pos uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// write to mmap
	i.writeMem(in, pos)
	// write to file
	_, err := i.buf.Write([]byte(in))
	if err != nil {
		return err
	}
	if err := binary.Write(i.buf, enc, pos); err != nil {
		return err
	}
	i.size += uint64(entwidth)
	i.latestKey = in
	return nil
}

func (i *index) Delete(in string) (poss []uint64, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size == 0 {
		return nil, io.EOF
	}

	if err := i.buf.Flush(); err != nil {
		return nil, err
	}

	if in == "" {
		// latest
		in = i.latestKey
	}

	poss, err = i.readAll(in)
	delete(i.mmap, in)
	delete(i.dmap, in)

	for off := uint64(0); off < i.size; off += entwidth {
		k := make([]byte, keyWidth)
		_, err = i.file.ReadAt(k, int64(off))
		if err != nil {
			return
		}
		key := string(k)
		if key != in {
			continue
		}
		empty := make([]byte, entwidth)
		_, err = i.file.WriteAt(empty, int64(off))
		if err != nil {
			return
		}
	}
	return
}

func (i *index) Flush() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.buf.Flush()
}

func (i *index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	err := i.buf.Flush()
	if err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) writeMem(in string, pos uint64) {
	prev, ok := i.mmap[in]
	if !ok {
		i.mmap[in] = pos
		return
	}
	// has prev data
	if prev == 1 {
		size := len(i.dmap[in])
		m := make([]uint64, size+1)
		copy(m, i.dmap[in])
		m[size] = pos
		// i.mmap[in] = 1
		i.dmap[in] = m
	} else {
		m := make([]uint64, 2)
		m[0] = prev
		m[1] = pos
		i.mmap[in] = 1
		i.dmap[in] = m
	}
}

func (i *index) readAll(in string) (poss []uint64, err error) {
	pos, ok := i.mmap[in]
	if !ok {
		return nil, errors.New("not found")
	}
	if pos != 1 {
		poss = make([]uint64, 1)
		poss[0] = pos
		return
	}
	// return latest
	poss = make([]uint64, len(i.dmap[in]))
	copy(poss, i.dmap[in])
	return
}

func (i *index) setup() error {
	i.mmap = make(map[string]uint64, i.size/entwidth)
	i.dmap = make(map[string][]uint64)
	if i.size == 0 {
		return io.EOF
	}
	for off := uint64(0); off < i.size; off += entwidth {
		k := make([]byte, keyWidth)
		_, err := i.file.ReadAt(k, int64(off))
		if err != nil {
			return err
		}
		p := make([]byte, posWidth)
		_, err = i.file.ReadAt(p, int64(off+keyWidth))
		if err != nil {
			return err
		}
		key := string(k)
		if key == "" {
			continue
		}
		pos := enc.Uint64(p)
		i.writeMem(key, pos)
	}

	return nil
}
