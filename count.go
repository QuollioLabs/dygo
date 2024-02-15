package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const opCount = "Count"

// Count executes a query operation on the DynamoDB table and returns the total number of items and the number of items that match the query filter.
//
// Example:
//
//	err = db.
//		GSI("gsi-name", "room", Equal("current")).
//		Filter("physical_name", KeyBeginsWith(prefix)).
//		AndFilter("logical_name", KeyBeginsWith(prefix)).
//		Count(context.Background())
//
// OR
//
//	err = db.
//		PK(PK).
//		SK(Equal(SK)).
//		Count(context.Background())
func (i *Item) Count(ctx context.Context) (int, int, error) {
	totalCount, filteredCount := 0, 0
	if i.err != nil {
		return totalCount, filteredCount, i.err
	}

	expr, err := i.getQueryExpression()
	if err != nil {
		return totalCount, filteredCount, dynamoError().method(opCount).message(err.Error())
	}

	input := dynamodb.QueryInput{
		TableName:                 aws.String(i.c.tableName),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	if i.useGSI {
		input.IndexName = aws.String(i.indexName)
	}
	return i.getAllPages(ctx, &input)
}

// getAllPages queries all pages of results for a given DynamoDB query input.
// It returns the total number of items and the number of items that match the query filter.
func (i *Item) getAllPages(ctx context.Context, input *dynamodb.QueryInput) (int, int, error) {
	totalCount, filteredCount := 0, 0
	paginator := dynamodb.NewQueryPaginator(i.c.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			if err := getDynamoDBError(opCount, err); err != nil {
				return totalCount, filteredCount, err
			}
			return totalCount, filteredCount, dynamoError().method(opCount).message(err.Error())
		}
		totalCount += int(output.ScannedCount)
		filteredCount += int(output.Count)
	}
	return totalCount, filteredCount, nil
}
