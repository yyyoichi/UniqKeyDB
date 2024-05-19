# TinyamoDB

## Design and Implementation of a Simplified DynamoDB

This document describes the design and implementation of a simplified version of DynamoDB. The following features have been implemented:

- [x] Saving data to local storage
- [x] Partitioned data
- [x] Using the primary key as the partition key
- [x] Overwriting with Put

Features not yet implemented:

- [ ] Accessing historical data
- [ ] Server and client implementation
- [ ] Distributed system
- [ ] Sort key
- [ ] Global Secondary Index (GSI)
- [ ] Other features

## How to use

```golang

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
```
