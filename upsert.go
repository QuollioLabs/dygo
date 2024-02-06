package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const opUpsert = "Upsert"

// Upsert updates or inserts an item into the DynamoDB table.
// It validates the item using user provided function, marshals it into a map using JSON tags,
// and then performs a PutItem operation on the DynamoDB table.
// If any error occurs during the process, it returns an error.
func (i *Item) Upsert(ctx context.Context) error {
	if i.err != nil {
		return i.err
	}

	err := i.item.Validate()
	if err != nil {
		return dynamoError().method(opUpsert).message(err.Error())
	}

	av, err := attributevalue.MarshalMap(i.item)
	if err != nil {
		return err
	}

	input := dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(i.c.tableName),
	}

	if _, err := i.c.client.PutItem(context.TODO(), &input); err != nil {
		if err := getDynamoDBError(opUpsert, err); err != nil {
			return err
		}
		return dynamoError().method(opUpsert).message(err.Error())
	}
	return nil
}
