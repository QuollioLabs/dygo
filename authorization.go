package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type output struct {
	Results []map[string]types.AttributeValue
	item    *Item
	ctx     context.Context
}

func newOutput(item *Item, ctx context.Context) *output {
	return &output{
		item: item,
		ctx:  ctx,
	}
}

func (o *output) Run() error {
	return o.item.err
}

// Unmarshal unmarshals the DynamoDB query results into the provided 'out' object,
// It is only used for queries and batch get operations and 'out' must be a slice of structs that implement the 'Out' interface.
// It filters the results based on the specified 'entityTypes'.
// It also performs authorization by calling the 'Authorize' method on the 'out' object.
//
// Example :
//
//	var data []dataItem
//	err = item.BatchGetAuthorizedItem(context.Background(), 10).
//		Unmarshal(&data, []string{"room"}).
//		Run()
//
// And dataItem struct must implement the Out interface which has a Authorize method.
//
//	func (d *dataSlice) Authorize(ctx context.Context) error {
//		// write your own logic
//		return nil
//	}
//
// Here Unmarshal will unmarshal only the items with _entity_type = "room".
func (o *output) Unmarshal(out out, entityTypes []string) *output {
	targetAttVals := []map[string]types.AttributeValue{}
	for _, result := range o.Results {
		switch v := result[o.item.c.gsis[0].partitionKey].(type) {
		case *types.AttributeValueMemberS:
			if stringExists(entityTypes, v.Value) {
				targetAttVals = append(targetAttVals, result)
			}
		}
	}

	if err := attributevalue.UnmarshalListOfMaps(targetAttVals, &out); err != nil {
		o.item.err = err
	}

	err := out.Authorize(o.ctx)
	if err != nil {
		o.item.err = dynamoError().method("authorization").message(err.Error())
	}
	return o
}

type out interface {
	Authorize(context.Context) error
}
