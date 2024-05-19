package tinyamodb

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestPartition(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const PARTITION_ID = 1
	var c Config
	c.Segment.MaxIndexBytes = entwidth
	c.Table.PartitionKey = "key"
	p, err := newPartition(dir, PARTITION_ID, c)
	require.NoError(t, err)

	want0, err := NewTinyamoDbItem(map[string]types.AttributeValue{
		"key":   &types.AttributeValueMemberS{Value: "key0"},
		"value": &types.AttributeValueMemberN{Value: "0"},
	}, c)
	require.NoError(t, err)
	_, err = p.Put(want0)
	require.NoError(t, err)

	want1, err := NewTinyamoDbItem(map[string]types.AttributeValue{
		"key":   &types.AttributeValueMemberS{Value: "key1"},
		"value": &types.AttributeValueMemberN{Value: "1"},
	}, c)
	require.NoError(t, err)
	_, err = p.Put(want1)
	require.NoError(t, err)
	require.Equal(t, 2, len(p.segments))

	got0, err := NewTinyamoDbItem(map[string]types.AttributeValue{
		"key": &types.AttributeValueMemberS{Value: "key0"},
	}, c)
	require.NoError(t, err)
	err = p.Read(got0)
	require.NoError(t, err)
	require.Equal(t, want0.UnixNano, got0.UnixNano)

	// not found
	err = p.Read(NewKeyTimeItem("key2"))
	require.Error(t, err)

	// close and open
	err = p.Close()
	require.NoError(t, err)

	p, err = newPartition(dir, PARTITION_ID, c)
	require.NoError(t, err)
	err = p.Read(got0)
	require.NoError(t, err)

	// overwrite
	got0, _ = NewTinyamoDbItem(map[string]types.AttributeValue{
		"key":   &types.AttributeValueMemberS{Value: "key0"},
		"value": &types.AttributeValueMemberN{Value: "0"},
	}, c)
	_, err = p.Put(got0)
	require.NoError(t, err)
	require.NotEqual(t, want0.UnixNano, got0.UnixNano)

	// delete
	_, err = p.Delete(want0)
	require.NoError(t, err)
	// read
	err = p.Read(want0)
	require.Error(t, err)
}
