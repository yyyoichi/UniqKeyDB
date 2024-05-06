package queue

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
	mmap      map[string]uint64 // [sha-256]uint64(storepos)
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
	pos, ok := i.mmap[in]
	if !ok {
		return 0, errors.New("not found")
	}
	return pos, nil
}

func (i *index) Write(in string, pos uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// write to mmap
	i.mmap[in] = pos
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

func (i *index) Delete(in string) (pos uint64, err error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size == 0 {
		return 0, io.EOF
	}

	if err := i.buf.Flush(); err != nil {
		return 0, err
	}

	if in == "" {
		// latest
		in = i.latestKey
	}
	pos, ok := i.mmap[in]
	if !ok {
		return 0, errors.New("not found")
	}
	delete(i.mmap, in)

	for off := uint64(0); off < i.size; off += entwidth {
		k := make([]byte, keyWidth)
		_, err := i.file.ReadAt(k, int64(off))
		if err != nil {
			return 0, err
		}
		key := string(k)
		if key != in {
			continue
		}
		empty := make([]byte, keyWidth)
		_, err = i.file.WriteAt(empty, int64(off))
		if err != nil {
			return 0, err
		}
		return pos, nil
	}
	return pos, nil
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

func (i *index) setup() error {
	i.mmap = make(map[string]uint64, i.size/entwidth)
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
		i.mmap[key] = enc.Uint64(p)
	}

	return nil
}
