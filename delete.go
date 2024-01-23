package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const opDelete = "Delete"

// Delete deletes the item from the DynamoDB table.
// It returns an error if there was an issue with the deletion process.
func (i *Item) Delete(ctx context.Context) error {

	if i.err != nil {
		return i.err
	}

	expr, err := i.deleteItemExpression()
	if err != nil {
		return dynamoError().method(opDelete).message(err.Error())
	}

	input := dynamodb.DeleteItemInput{
		TableName:                 aws.String(i.c.tableName),
		Key:                       i.key,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}
	if _, err := i.c.client.DeleteItem(context.TODO(), &input); err != nil {
		if err := getDynamoDBError(opDelete, err); err != nil {
			return err
		}
		return dynamoError().method(opDelete).message(err.Error())
	}
	return nil
}
