package queue

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type segment struct {
	store *store
	index *index

	uuid   string
	config Config
}

func newSegment(dir string, uid string, c Config) (*segment, error) {
	if uid == "" {
		uid = uuid.NewString()
	}
	s := &segment{
		uuid:   uid,
		config: c,
	}
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%s.store", s.uuid)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%s.index", s.uuid)),
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *segment) Read(in string) ([]byte, error) {
	pos, err := s.index.Read(in)
	if err != nil {
		return nil, err
	}
	return s.store.Read(pos)
}

func (s *segment) Write(in string, data []byte) error {
	_, pos, err := s.store.Append(data)
	if err != nil {
		return err
	}
	if err = s.index.Write(in, pos); err != nil {
		return err
	}
	return nil
}

func (s *segment) Delete(in string) error {
	_, err := s.index.Delete(in)
	return err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return nil
	}
	if err := s.store.Close(); err != nil {
		return nil
	}
	return nil
}
