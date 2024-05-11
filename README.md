# TinyamoDB

Degraded version of DynamoDB.

```golang
type TinyamoDb interface {
    PutKey(context.Context, string) (*PutKeyItemOutput, error)
    DeleteKey(context.Context, string) (*DeleteKeyItemOutput, error)
    ReadKey(context.Context, string) (*ReadKeyItemOutput, error)
    Close() error
}
```

## How to use

```golang

db, err := tinyamodb.New("/tmp", tinyamodb.Config{})
if err != nil {
    panic(err)
}

_, err := db.PutKey(ctx, "item1")
if err != nil {
    panic(err)
}

output, err := db.ReadKey(ctx, "item1")
if err != nil {
    panic(err)
}
fmt.Print(*output.Key) // item1

output, err = db.ReadKey(ctx, "item2")
if err != nil {
    panic(err)
}
fmt.Print(ouput.Key) // <nil>

```
