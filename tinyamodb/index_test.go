package tinyamodb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	key = "_123456789_123456789_123456789_123456789_123456789_123456789_12" // len = 63
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_write_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	idx, err := newIndex(f)
	require.NoError(t, err)

	testWriteIndex(t, idx)
	testReadIndex(t, idx)

	err = idx.Flush()
	require.NoError(t, err)

	idx, err = newIndex(f)
	require.NoError(t, err)
	testReadIndex(t, idx)

	err = idx.Write(key+"z", 4)
	require.NoError(t, err)
	_, err = idx.Delete(key + "z")
	require.NoError(t, err)
	testReadIndex(t, idx)

	testDeleteIndex(t, idx)
	require.Equal(t, 0, len(idx.mmap))
	require.Equal(t, 0, len(idx.dmap))
}

func testWriteIndex(t *testing.T, idx *index) {
	t.Helper()
	var rate uint64 = 10
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		err := idx.Write(k, i*rate)
		require.NoError(t, err)
	}
	rate = 11
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		err := idx.Write(k, i*rate)
		require.NoError(t, err)
	}
}

func testReadIndex(t *testing.T, idx *index) {
	t.Helper()
	var rate uint64 = 11
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		pos, err := idx.Read(k)
		require.NoError(t, err)
		require.Equal(t, i*rate, pos)
	}
}

func testDeleteIndex(t *testing.T, idx *index) {
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		ppos, err := idx.Delete(k)
		require.NoError(t, err)
		require.Equal(t, []uint64{i * 10, i * 11}, ppos)
	}
}
