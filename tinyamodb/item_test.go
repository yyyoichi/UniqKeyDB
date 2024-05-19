package tinyamodb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItem(t *testing.T) {
	var e encoder
	var d decoder

	t.Run("string", func(t *testing.T) {
		t.Parallel()
		want := "hello world"
		var b = new(bytes.Buffer)
		err := e.encodeString(want, b)
		require.NoError(t, err)
		got, err := d.decodeString(b)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("string set", func(t *testing.T) {
		t.Parallel()
		want := []string{"hoge", "fuga", "bar", "foo"}
		var b = new(bytes.Buffer)
		err := e.encodeSSet(want, b)
		require.NoError(t, err)
		got, err := d.decodeSSet(b)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("bytes", func(t *testing.T) {
		t.Parallel()
		want := []byte("hello world")
		var b = new(bytes.Buffer)
		err := e.encodeBytes(want, b)
		require.NoError(t, err)
		got, err := d.decodeBytes(b)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("bytes set", func(t *testing.T) {
		t.Parallel()
		want := [][]byte{[]byte("hoge"), []byte("fuga"), []byte("bar"), []byte("foo")}
		var b = new(bytes.Buffer)
		err := e.encodeBSet(want, b)
		require.NoError(t, err)
		got, err := d.decodeBSet(b)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("bool", func(t *testing.T) {
		t.Parallel()
		want := true
		var b = new(bytes.Buffer)
		err := e.encodeBool(want, b)
		require.NoError(t, err)
		got, err := d.decodeBool(b)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
}
