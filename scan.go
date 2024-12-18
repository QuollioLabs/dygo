package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const opScan = "Scan"

// Scan performs a scan operation on the DynamoDB table and returns an Output object containing the results or an error if the operation fails.
//
// Preconditions:
// - The InitScan() method **must** be called before invoking Scan.
//
// Usage Notes:
// - Retrieved items can be accessed using the Unmarshall() method.
// - In case of paginated scan or if the size exceeds 1MB, the LastEvaluatedKey will be returned in the Output object.
// - The Run() method fill data into the data slice.
// - The RunAndFetchLastKey() method returns the last evaluated key and fill data into the data slice.
//
// Example:
//
//	lek, err = db.
//		InitScan().
//		Filter("physical_name", KeyBeginsWith(prefix)).
//		AndFilter("logical_name", KeyBeginsWith(prefix)).
//		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
//		Limit(limit).
//		LastEvaluatedKey(lek).
//	    Scan(context.Background()).
//		Unmarshal(&data).
//		Run() OR RunAndFetchLastKey()
func (i *Item) Scan(ctx context.Context) *output {
	result := newOutput(i, ctx)
	if i.err != nil {
		return result
	}

	expr, err := i.getScanExpression()
	if err != nil {
		result.item.err = dynamoError().method(opScan).message(err.Error())
		return result
	}

	input := dynamodb.ScanInput{
		TableName:                 aws.String(i.c.tableName),
		ProjectionExpression:      expr.Projection(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	if i.pagination.lastEvaluatedKey != nil && len(i.pagination.lastEvaluatedKey) > 0 {
		input.ExclusiveStartKey = i.pagination.lastEvaluatedKey
	}

	out, err := i.scan(ctx, &input, result)
	if err != nil {
		result.item.err = err
	}
	return out
}

// scan scans all pages of results for a given DynamoDB scan input.
func (i *Item) scan(ctx context.Context, input *dynamodb.ScanInput, result *output) (*output, error) {
	paginator := dynamodb.NewScanPaginator(i.c.client, input)
	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			if err := getDynamoDBError(opScan, err); err != nil {
				return nil, err
			}
			return nil, dynamoError().method(opScan).message(err.Error())
		}
		items = append(items, output.Items...)
		if i.pagination.limit > 0 && len(items) >= int(i.pagination.limit) {
			result.Results = append(result.Results, items[:i.pagination.limit]...)
			result.LastEvaluatedKey = i.createLastKey(result.Results[len(result.Results)-1])
			return result, nil
		}
	}
	result.Results = append(result.Results, items...)
	return result, nil
}

func (i *Item) createLastKey(lastItem map[string]types.AttributeValue) map[string]types.AttributeValue {
	lastKey := make(map[string]types.AttributeValue)
	for k, v := range lastItem {
		if k == i.c.partitionKey || (k == i.c.sortKey && i.c.sortKey != "") {
			lastKey[k] = v
		}
	}
	return lastKey
}
