package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const opCreate = "Create"

// Create creates a new item in DynamoDB.
// It validates the item using the user defined Validate() method.
//
// Example :
//
//	newData := dataItem{
//		PK:           PK,
//		SK:           SK,
//		EntityType:   "room",
//		PhysicalName: "physical_name_11",
//		LogicalName:  "logical_name_11",
//	}
//
//	err = db.
//		Item(newData).
//		Create(context.Background())
//
// Important : dataItem must implement Validate() method.
func (i *Item) Create(ctx context.Context) error {
	if i.err != nil {
		return i.err
	}
	av, err := marshalMapUsingJSONTags(i.item)
	if err != nil {
		return dynamoError().method(opCreate).message(err.Error())
	}

	err = i.item.Validate()
	if err != nil {
		return dynamoError().method(opCreate).message(err.Error())
	}

	expr, err := i.createItemExpression()
	if err != nil {
		return dynamoError().method(opCreate).message(err.Error())
	}
	input := dynamodb.PutItemInput{
		Item:                      av,
		TableName:                 aws.String(i.c.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}

	if _, err := i.c.client.PutItem(context.TODO(), &input); err != nil {
		if err := getDynamoDBError(opCreate, err); err != nil {
			return err
		}
		return dynamoError().method(opCreate).message(err.Error())
	}
	return nil
}
