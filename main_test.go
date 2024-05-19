package tinyamodb

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/yyyoichi/tinyamodb/tinyamodb"
)

func Example() {
	// init db
	var c tinyamodb.Config
	c.Table.PartitionKey = "pk" // primary-key

	db, err := tinyamodb.New("/tmp/tinyamodb", c)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.PutItem(ctx, &tinyamodb.PutItemInput{Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "USER#1"},
		"name": &types.AttributeValueMemberS{Value: "Taro"},
		"age":  &types.AttributeValueMemberN{Value: "20"},
	}})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	output, err := db.GetItem(ctx, &tinyamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "USER#1"},
		},
	})
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	name := output.Item["name"].(*types.AttributeValueMemberS)
	age := output.Item["age"].(*types.AttributeValueMemberN)
	fmt.Println(name.Value)
	fmt.Println(age.Value)
	// Output:
	// Taro
	// 20
}
