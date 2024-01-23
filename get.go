package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const opGet = "Get"

// GetItem retrieves an item from DynamoDB based on the provided key and stores the result in the 'out' parameter.
// It returns an error if there was an issue with the retrieval or unmarshaling of the item.
//
// Example:
//
//	d := dataItem{}
//	err = db.
//		PK(PK).
//		SK(Equal(SK)).
//		GetItem(context.Background(), &d)
func (i *Item) GetItem(ctx context.Context, out interface{}) error {
	if i.err != nil {
		return i.err
	}

	expr, err := i.getItemExpression()
	if err != nil {
		return dynamoError().method(opGet).message(err.Error())
	}

	input := dynamodb.GetItemInput{
		Key:       i.key,
		TableName: aws.String(i.c.tableName),
	}

	if expr.Projection() != nil {
		input.ProjectionExpression = expr.Projection()
		input.ExpressionAttributeNames = expr.Names()
	}

	output, err := i.c.client.GetItem(ctx, &input)
	if err != nil {
		if err := getDynamoDBError(opGet, err); err != nil {
			return err
		}
		return dynamoError().method(opGet).message(err.Error())
	}

	if err := attributevalue.UnmarshalMap(output.Item, &out); err != nil {
		return err
	}

	return nil
}

// GetAuthorizedItem retrieves an authorized item from DynamoDB based on the provided key.
// It performs authorization checks on the retrieved item with user defined Authorize() function before returning it.
// If there is an error during the retrieval or authorization process, it returns the corresponding error.
//
// Example:
//
//	func (d *dataItem) Authorize(ctx context.Context) error {
//		return nil
//	}
//	d := dataItem{}
//	err = db.
//		PK(PK).
//		SK(Equal(SK)).
//		GetItem(context.Background(), &d)
func (i *Item) GetAuthorizedItem(ctx context.Context, out out) error {
	if i.err != nil {
		return i.err
	}

	expr, err := i.getItemExpression()
	if err != nil {
		return dynamoError().method(opGet).message(err.Error())
	}

	input := dynamodb.GetItemInput{
		Key:       i.key,
		TableName: aws.String(i.c.tableName),
	}

	if expr.Projection() != nil {
		input.ProjectionExpression = expr.Projection()
		input.ExpressionAttributeNames = expr.Names()
	}

	output, err := i.c.client.GetItem(ctx, &input)
	if err != nil {
		return getDynamoDBError(opGet, err)
	}

	if err := attributevalue.UnmarshalMap(output.Item, &out); err != nil {
		return err
	}

	err = out.Authorize(ctx)
	if err != nil {
		return err
	}
	return nil
}
