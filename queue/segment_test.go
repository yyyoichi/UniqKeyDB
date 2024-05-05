package queue

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	c := Config{}
	want := func() []byte {
		s := "Hello world!"
		return bytes.NewBufferString(s).Bytes()
	}()

	// case 1
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entwidth * 3

	s, err := newSegment(dir, "test", c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		k := fmt.Sprintf("%s%d", key, i)
		err := s.Write(k, want)
		require.NoError(t, err)

		got, err := s.Read(k)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	require.True(t, s.IsMaxed())
	require.NoError(t, s.Close())

	// case 2
	c.Segment.MaxStoreBytes = uint64(len(want)+lenWidth) * 4
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, "test", c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

	k := fmt.Sprintf("%s%d", key, 3)
	err = s.Write(k, want)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	require.NoError(t, s.Remove())

	s, err = newSegment(dir, "test", c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
	require.NoError(t, s.Close())
}
