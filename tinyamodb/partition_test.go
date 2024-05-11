package tinyamodb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartition(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var c Config
	c.Segment.MaxIndexBytes = entwidth
	p, err := newPartition(dir, 1, c)
	require.NoError(t, err)

	want0 := NewKeyTimeItem("key0")
	_, err = p.Put(want0)
	require.NoError(t, err)

	_, err = p.Put(NewKeyTimeItem("key1"))
	require.NoError(t, err)
	require.Equal(t, 2, len(p.segments))

	got0 := NewKeyTimeItem("key0")
	err = p.Read(got0)
	require.NoError(t, err)
	require.Equal(t, want0.UnixNano, got0.UnixNano)

	// not found
	err = p.Read(NewKeyTimeItem("key2"))
	require.Error(t, err)

	err = p.Close()
	require.NoError(t, err)

	p, err = newPartition(dir, 1, c)
	require.NoError(t, err)
	err = p.Read(got0)
	require.NoError(t, err)

	// overwrite
	got0 = NewKeyTimeItem("key0")
	old, err := p.Put(got0)
	require.NoError(t, err)
	require.Equal(t, want0.UnixNano, old.(*KeyTimeItem).UnixNano)
	require.NotEqual(t, want0.UnixNano, got0.UnixNano)

	// delete
	old, err = p.Delete(want0)
	require.NoError(t, err)
	require.NotNil(t, old)
	// read
	err = p.Read(want0)
	require.Error(t, err)
}
