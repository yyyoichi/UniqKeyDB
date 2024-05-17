package tinyamodb

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

var getAttributeValues = func() []map[string]types.AttributeValue {
	return []map[string]types.AttributeValue{
		{
			"key": &types.AttributeValueMemberS{
				Value: "first",
			},
			"doc": &types.AttributeValueMemberL{
				Value: []types.AttributeValue{
					&types.AttributeValueMemberN{
						Value: "1.23",
					},
					&types.AttributeValueMemberN{
						Value: "98.7",
					},
				},
			},
		},
		{
			"key": &types.AttributeValueMemberS{
				Value: "second",
			},
			"doc": &types.AttributeValueMemberBOOL{
				Value: true,
			},
		},
		{
			"key": &types.AttributeValueMemberS{
				Value: "thrid",
			},
			"doc": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"name": &types.AttributeValueMemberS{
						Value: "taro",
					},
					"age": &types.AttributeValueMemberN{
						Value: "10",
					},
				},
			},
		},
	}
}

func TestTinyamoDb(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-db")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var c Config
	c.Partition.Num = 8
	c.Table.PartitionKey = "key"
	tdb, err := New(dir, c)
	require.NoError(t, err)
	database := (tdb).(*db)

	// put
	testPutItem(t, database)
	testGetItem(t, database)

	// overwrite
	testPutItem(t, database)
	testGetItem(t, database)

	err = database.Close()
	require.NoError(t, err)

	tdb, err = New(dir, c)
	require.NoError(t, err)
	database = (tdb).(*db)
	testGetItem(t, database)

	// delete
	testDeleteItem(t, database)
}

func testPutItem(t *testing.T, db *db) {
	t.Helper()
	for _, v := range getAttributeValues() {
		_, err := db.PutItem(context.Background(), &PutItemInput{
			Item: v,
		})
		require.NoError(t, err)
	}
}

func testGetItem(t *testing.T, db *db) {
	for _, v := range getAttributeValues() {
		output, err := db.GetItem(context.Background(), &GetItemInput{
			Key: v,
		})
		require.NoError(t, err)
		require.Equal(t, v, output.Item)
	}
}

func testDeleteItem(t *testing.T, db *db) {
	t.Helper()
	for _, v := range getAttributeValues() {
		_, err := db.DeleteItem(context.Background(), &DeleteItemInput{
			Key: v,
		})
		require.NoError(t, err)
	}
	for _, v := range getAttributeValues() {
		output, err := db.GetItem(context.Background(), &GetItemInput{
			Key: v,
		})
		require.NoError(t, err)
		require.Nil(t, output.Item)
	}
}
