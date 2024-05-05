package queue

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	key = "_123456789_123456789_123456789_" // len = 31
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_write_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	idx, err := newIndex(f)
	require.NoError(t, err)

	testWrite(t, idx)
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
}

func testWrite(t *testing.T, idx *index) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		err := idx.Write(k, i)
		require.NoError(t, err)
	}
}

func testReadIndex(t *testing.T, idx *index) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		pos, err := idx.Read(k)
		require.NoError(t, err)
		require.Equal(t, i, pos)
	}
}
