package dygo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

const (
	maxRetriesDelete = 3
	opBatchDelete    = "BatchDelete"
)

// BatchDeleteItem deletes multiple items in batches.
// It takes a context and the number of threads to use for parallel processing.
// It returns an error if any of the batch operations fail.
//
// Example :
//
//	item := new(Item)
//	for _, gId := range gIds {
//		db.PK(gId).SK(Equal(SK)).AddBatchDeleteItem(item)
//	}
//
//	err = item.BatchDeleteItem(context.Background(), 10)
//	if err != nil {
//		log.Fatal(err)
//	}
func (i *Item) BatchDeleteItem(ctx context.Context, threadCount int) error {
	if i.err != nil {
		return i.err
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(threadCount)

	for _, batch := range i.batchData.batchDelete {
		batch := batch
		g.Go(func() error {
			return i.processBatchDelete(ctx, batch)
		})
	}

	// Wait for all batch operations to complete
	if err := g.Wait(); err != nil {
		return dynamoError().method(opBatchDelete).message(err.Error())
	}

	return nil
}

// processBatchDelete processes a batch of write requests for deleting items in DynamoDB.
// It takes a context and a map of write requests as input and returns an error if any.
// The function retries the unprocessed items if there are any, up to a maximum number of retries.
func (i *Item) processBatchDelete(ctx context.Context, batch map[string][]types.WriteRequest) error {
	var retries int
	for {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		}

		result, err := i.c.client.BatchWriteItem(ctx, input)
		if err != nil {
			return dynamoError().method(opBatchDelete).message(err.Error())
		}

		if len(result.UnprocessedItems) == 0 || retries >= maxRetriesDelete {
			break
		}

		// Retry unprocessed items
		batch = result.UnprocessedItems
		retries++
	}

	// Simulate a delay to avoid throttling
	time.Sleep(time.Second * 1)
	return nil
}
