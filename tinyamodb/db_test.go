package tinyamodb

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var keys = func() []string {
	keys := make([]string, 10)
	for i := range keys {
		keys[i] = uuid.NewString()
	}
	return keys
}()

func TestDb(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-db")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var c Config
	c.Partition.Num = 8
	db, err := New(dir, c)
	require.NoError(t, err)

	// put
	testPutKey(t, db)
	testReadKey(t, db)

	// overwrite
	testPutKey(t, db)
	testReadKey(t, db)

	err = db.Close()
	require.NoError(t, err)

	db, err = New(dir, c)
	require.NoError(t, err)
	testReadKey(t, db)

	// delete
	testDeleteKey(t, db)
}

func testPutKey(t *testing.T, db TinyamoDb) {
	t.Helper()
	for _, key := range keys {
		_, err := db.PutKey(context.Background(), key)
		require.NoError(t, err)
	}
}

func testReadKey(t *testing.T, db TinyamoDb) {
	for _, key := range keys {
		output, err := db.ReadKey(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, key, *output.Key)
	}
}

func testDeleteKey(t *testing.T, db TinyamoDb) {
	t.Helper()
	for _, key := range keys {
		_, err := db.DeleteKey(context.Background(), key)
		require.NoError(t, err)
	}
	for _, key := range keys {
		output, err := db.ReadKey(context.Background(), key)
		require.NoError(t, err)
		require.Nil(t, output.Key)
	}
}
