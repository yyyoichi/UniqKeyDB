package tinyamodb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type db struct {
	// partition id start with 1
	partitions map[int]*partition
	c          Config
}

func New(dir string, c Config) (*db, error) {
	if _, err := os.Stat(dir); err != nil {
		if err = os.Mkdir(dir, 0755); err != nil {
			return nil, err
		}
	}

	db := &db{
		partitions: make(map[int]*partition),
		c:          c,
	}

	// read from children dir.
	// cannot change partition num after init the database.
	children, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		if !child.IsDir() {
			continue
		}
		name := child.Name()
		id, _ := strconv.Atoi(name)
		if id == 0 {
			continue
		}
		db.partitions[id], err = newPartition(dir, id, c)
		if err != nil {
			return nil, err
		}
	}

	if l := len(db.partitions); l == 0 {
		// create
		if c.Partition.Num == 0 {
			c.Partition.Num = 10
		}
		for i := 1; i <= int(c.Partition.Num); i++ {
			db.partitions[i], err = newPartition(dir, i, c)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// serial
		for i := 1; i <= l; i++ {
			_, found := db.partitions[i]
			if !found {
				return nil, fmt.Errorf("unexpected error: partition '%d' is not found", i)
			}
		}
	}

	return db, nil
}

func (db *db) Close() error {
	for _, p := range db.partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *db) GetItem(ctx context.Context, input *GetItemInput) (*GetItemOutput, error) {
	item, err := NewTinyamoDbItem(input.Key, db.c)
	if err != nil {
		return nil, err
	}
	p := db.determinePartition(item.sha256Key)

	output := &tinyamodbItem{
		sha256Key:    item.sha256Key,
		strSha256Key: item.strSha256Key,
		UnixNano:     0,
		Item:         nil,
	}
	err = p.Read(output)
	if err != nil && !errors.Is(err, io.EOF) {
		if errors.Is(err, io.EOF) {
			return &GetItemOutput{Item: nil}, nil
		}
		return nil, err
	}
	return &GetItemOutput{Item: output.Item}, nil
}

func (db *db) PutItem(ctx context.Context, input *PutItemInput) (*PutItemOutput, error) {
	item, err := NewTinyamoDbItem(input.Item, db.c)
	if err != nil {
		return nil, err
	}
	p := db.determinePartition(item.sha256Key)
	_, err = p.Put(item)
	if err != nil {
		return nil, err
	}
	return &PutItemOutput{}, nil
}

func (db *db) DeleteItem(ctx context.Context, input *DeleteItemInput) (*DeleteItemOutput, error) {
	item, err := NewTinyamoDbItem(input.Key, db.c)
	if err != nil {
		return nil, err
	}
	p := db.determinePartition(item.sha256Key)
	_, err = p.Delete(item)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &DeleteItemOutput{}, nil
}

func (db *db) determinePartition(sha256key []byte) *partition {
	v := binary.BigEndian.Uint32(sha256key[:4])
	id := int(v) % len(db.partitions)
	// partition id start with 1
	return db.partitions[id+1]
}
