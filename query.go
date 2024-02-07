package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const opQuery = "Query"

// QueryAuthorizeItem executes a query operation on the DynamoDB table.
// The method returns an Output object containing the query results or an error if the query fails.
// Items can be retrieved from the Output object using Unmarshall().
//
// Example:
//
//	err = db.
//		GSI("gsi-name", "room", Equal("current")).
//		Filter("physical_name", KeyBeginsWith(prefix)).
//		AndFilter("logical_name", KeyBeginsWith(prefix)).
//		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
//		Limit(2).
//		LastEvaluatedKey(lek).
//		Query(context.Background()).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (i *Item) Query(ctx context.Context) *output {
	result := newOutput(i, ctx)
	if i.err != nil {
		return result
	}

	expr, err := i.getQueryExpression()
	if err != nil {
		result.item.err = dynamoError().method(opQuery).message(err.Error())
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

	if i.pagination.lastEvaluatedKey != nil && len(i.pagination.lastEvaluatedKey) > 0 {
		input.ExclusiveStartKey = i.pagination.lastEvaluatedKey
	}

	if i.pagination.desc {
		input.ScanIndexForward = aws.Bool(false)
	}

	if i.pagination.limit > 0 {
		out, err := i.querySinglePage(ctx, &input, result)
		if err != nil {
			result.item.err = err
		}
		return out
	}
	out, err := i.queryAllPages(ctx, &input, result)
	if err != nil {
		result.item.err = err
	}
	return out
}

// querySinglePage queries a single page of items from DynamoDB using the provided input.
func (i *Item) querySinglePage(ctx context.Context, input *dynamodb.QueryInput, result *output) (*output, error) {
	output, err := i.c.client.Query(ctx, input)
	if err != nil {
		if err := getDynamoDBError(opQuery, err); err != nil {
			return nil, err
		}
		return nil, dynamoError().method(opQuery).message(err.Error())
	}
	// fetch with pagination
	if i.pagination.limit > 0 {
		if len(output.Items) >= int(i.pagination.limit) {
			// if total items is over the page size, limit items
			result.Results = append(result.Results, output.Items[:i.pagination.limit]...)
		} else {
			result.Results = append(result.Results, output.Items...)
		}
	}
	return result, nil
}

// queryAllPages queries all pages of results for a given DynamoDB query input.
func (i *Item) queryAllPages(ctx context.Context, input *dynamodb.QueryInput, result *output) (*output, error) {
	paginator := dynamodb.NewQueryPaginator(i.c.client, input)
	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			if err := getDynamoDBError(opQuery, err); err != nil {
				return nil, err
			}
			return nil, dynamoError().method(opQuery).message(err.Error())
		}
		items = append(items, output.Items...)
	}
	result.Results = append(result.Results, items...)
	return result, nil
}
