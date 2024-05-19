package tinyamodb

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

type PutKeyItemOutput struct {
}

type DeleteKeyItemOutput struct {
}

type ReadKeyItemOutput struct {
	Key *string
}

type PutItemInput struct {
	Item map[string]types.AttributeValue
}

type PutItemOutput struct {
}

type GetItemInput struct {
	Key map[string]types.AttributeValue
}

type GetItemOutput struct {
	Item map[string]types.AttributeValue
}

type DeleteItemInput struct {
	Key map[string]types.AttributeValue
}

type DeleteItemOutput struct {
}
