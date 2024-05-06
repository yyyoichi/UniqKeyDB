package queue

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type partition struct {
	mu     sync.RWMutex
	dir    string
	config Config

	activeSegment *segment
	segments      []*segment
}

func newPartition(dir string, id int, c Config) (*partition, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	p := &partition{
		dir:    fmt.Sprintf("%s/%d", dir, id),
		config: c,
	}
	if _, err := os.Stat(p.dir); err != nil {
		if err = os.Mkdir(p.dir, 0755); err != nil {
			return nil, err
		}
	}

	return p, p.setup()
}

func (p *partition) Put(item Item) (old Item, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := item.StrSHA2526Key()
	old = item.Clone()
	s, err := p.read(old)
	// already exist in deactive segment
	if s != nil && s != p.activeSegment {
		if err := s.Delete(key); err != nil {
			return old, err
		}
	}
	// in active segment
	// (over)write
	err = p.write(item)
	return old, err
}

func (p *partition) Read(item Item) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, err := p.read(item)
	return err
}

func (p *partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, s := range p.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *partition) read(item Item) (*segment, error) {
	key := item.StrSHA2526Key()

	for _, s := range p.segments {
		data, _ := s.Read(key)
		if len(data) > 0 {
			if err := item.Unmarshal(data); err == nil {
				return s, nil
			}
		}
	}
	return nil, io.EOF
}

func (p *partition) write(item Item) error {
	if p.activeSegment.IsMaxed() {
		if err := p.newSegment(""); err != nil {
			return err
		}
	}

	key := item.StrSHA2526Key()
	data, err := item.Value()
	if err != nil {
		return err
	}
	return p.activeSegment.Write(key, data)
}

func (p *partition) setup() error {
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	segmentUUid := make(map[string]int, len(files)/2)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		uid := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		segmentUUid[uid]++
	}

	for uid, count := range segmentUUid {
		if count != 2 {
			continue
		}
		if err := p.newSegment(uid); err != nil {
			return err
		}
	}

	if p.activeSegment == nil {
		if err := p.newSegment(""); err != nil {
			return err
		}
	}
	return nil
}

func (l *partition) newSegment(uid string) error {
	s, err := newSegment(l.dir, uid, l.config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	if !s.IsMaxed() {
		l.activeSegment = s
	}
	return nil
}
