package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const opCountWithLimt = "CountWithLimt"

// CountWithLimt accepts an argument lilim, it executes the query operation on the DynamoDB table, counts items, if items are more than limit it returns limt else returns actual item count that match the query filter.
//
// Example:
//
//	err = db.
//		GSI("gsi-name", "room", Equal("current")).
//		Filter("physical_name", KeyBeginsWith(prefix)).
//		AndFilter("logical_name", KeyBeginsWith(prefix)).
//		CountWithLimt(context.Background(), 5)
func (i *Item) CountWithLimt(ctx context.Context, limit int) (int, int, error) {
	totalCount, filteredCount := 0, 0
	if i.err != nil {
		return totalCount, filteredCount, i.err
	}

	expr, err := i.getQueryExpression()
	if err != nil {
		return totalCount, filteredCount, dynamoError().method(opCountWithLimt).message(err.Error())
	}

	input := dynamodb.QueryInput{
		TableName:                 aws.String(i.c.tableName),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Select:                    types.SelectCount,
	}

	if i.useGSI {
		input.IndexName = aws.String(i.indexName)
	}
	return i.getAllPagesForCountWithLimt(ctx, &input, limit)
}

// getAllPagesForCountWithLimt queries all pages of results for a given DynamoDB query input.
// It returns the total number of items if they are less than limit else retuens limit.
func (i *Item) getAllPagesForCountWithLimt(ctx context.Context, input *dynamodb.QueryInput, limit int) (int, int, error) {
	totalCount, filteredCount := 0, 0
	paginator := dynamodb.NewQueryPaginator(i.c.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			if err := getDynamoDBError(opCountWithLimt, err); err != nil {
				return totalCount, filteredCount, err
			}
			return totalCount, filteredCount, dynamoError().method(opCountWithLimt).message(err.Error())
		}
		totalCount += int(output.ScannedCount)
		filteredCount += int(output.Count)
		if filteredCount > limit {
			filteredCount = limit
			break
		}
	}
	return totalCount, filteredCount, nil
}
