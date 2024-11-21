package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type output struct {
	Results    []map[string]types.AttributeValue
	item       *Item
	ctx        context.Context
	bypassAuth bool
}

func newOutput(item *Item, ctx context.Context) *output {
	return &output{
		item:    item,
		ctx:     ctx,
		Results: make([]map[string]types.AttributeValue, 0),
	}
}

func (o *output) Run() error {
	if o == nil || o.item == nil {
		return nil
	}
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
func (o *output) Unmarshal(out Out, entityTypes []string) *output {
	if o == nil || o.item == nil || o.item.err != nil || o.Results == nil {
		return o
	}

	targetAttVals := []map[string]types.AttributeValue{}
	for _, result := range o.Results {
		switch v := result[o.getObjectTypeAttribute()].(type) {
		case *types.AttributeValueMemberS:
			key := getSplittedKey(v.Value, o.item.c.keySeparator)
			if stringExists(entityTypes, key) {
				targetAttVals = append(targetAttVals, result)
			}
		}
	}

	if err := attributevalue.UnmarshalListOfMaps(targetAttVals, &out); err != nil {
		o.item.err = err
	}

	if !o.bypassAuth {
		err := out.Authorize(o.ctx)
		if err != nil {
			o.item.err = dynamoError().method("authorization").message(err.Error())
		}
	}
	return o
}

func (o *output) getObjectTypeAttribute() string {
	if o.item.customObjectTypeAttribute == "" {
		return getPartitionKey(o.item)
	}

	return o.item.customObjectTypeAttribute
}

// WithCustomObjectTypeAttribute is used to specify a custom attribute name that is used to filter the results.
// Example :
//
//	var data []dataItem
//	err = item.BatchGetAuthorizedItem(context.Background(), 10).
//		WithCustomObjectTypeAttribute("custom_entity_type").
//		Unmarshal(&data, []string{"room"}). // the items with custom_entity_type = "room" will be unmarshaled
//		Run()
func (o *output) WithCustomObjectTypeAttribute(attr string) *output {
	o.item.customObjectTypeAttribute = attr
	return o
}

// BypassAuthorization is used to bypass the authorization process inside Unmarshal function.
// Example :
//
//	var data []dataItem
//	err = item.BatchGetAuthorizedItem(context.Background(), 10).
//	    BypassAuth().
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (o *output) BypassAuthorization() *output {
	o.bypassAuth = true
	return o
}

// Out is an interface that must be implemented by the struct that is used to unmarshal the DynamoDB results.
// It has a Authorize method that is used to perform authorization on the retrieved items.
type Out interface {
	Authorize(context.Context) error
}
