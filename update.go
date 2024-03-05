package dygo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

const opUpdate = "Update"

func (i *Item) Update(ctx context.Context, n int) error {
	if i.err != nil {
		return i.err
	}

	// Calculate batch size per goroutine
	batchSize := (len(i.batchData.updateItems) + n - 1) / n

	g, ctx := errgroup.WithContext(ctx)

	for j := 0; j < n; j++ {
		// Calculate start and end indices for the current batch
		start := j * batchSize
		end := start + batchSize
		if end > len(i.batchData.updateItems) {
			end = len(i.batchData.updateItems)
		}

		g.Go(func() error {
			for index := range i.batchData.updateItems[start:end] {
				// Construct an expression for each item
				expr, err := i.getUpdateItemExpression(start + index)
				if err != nil {
					return dynamoError().method(opUpdate).message(err.Error())
				}

				key := i.getUpdateItemKey(start + index)

				updateItemInput := &dynamodb.UpdateItemInput{
					TableName:                 aws.String(i.c.tableName),
					Key:                       key,
					UpdateExpression:          expr.Update(),
					ExpressionAttributeNames:  expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					ReturnValues:              types.ReturnValueUpdatedNew,
				}

				_, err = i.c.client.UpdateItem(ctx, updateItemInput)
				if err != nil {
					return dynamoError().method(opUpdate).message(err.Error())
				}
			}
			return nil
		})
	}

	// Wait for all goroutines to complete and return the first encountered error
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
