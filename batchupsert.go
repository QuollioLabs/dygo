package dygo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

const opBatchUpsert = "BatchUpsert"

const maxRetriesUpsert = 3

// BatchUpsertItem performs batch upsert operations on items.
// It takes a context and the number of threads to use for parallel processing.
// It returns an error if any of the batch operations fail.
//
// Example :
//
//	newItem := new(Item)
//	for i := 0; i < 5; i++ {
//		d := dataItem{
//			PK:           gId,
//			SK:           "current",
//			PhysicalName: "physical_name_1145",
//			LogicalName:  "logical_name_1145",
//			EntityType:   "room",
//		}
//		db.Item(d).AddBatchUpsertItem(newItem)
//	}
//	err = newItem.BatchUpsertItem(context.Background(), 10)
//	if err != nil {
//		log.Fatal(err)
//	}
func (i *Item) BatchUpsertItem(ctx context.Context, threadCount int) error {
	if i.err != nil {
		return i.err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(threadCount)

	for _, batch := range i.batchData.batchPut {
		batch := batch
		g.Go(func() error {
			return i.processBatchUpsert(ctx, batch)
		})
	}

	// Wait for all batch operations to complete
	if err := g.Wait(); err != nil {
		return dynamoError().method(opBatchUpsert).message(err.Error())
	}
	return nil
}

// processBatchUpsert processes a batch of write requests and performs batch upsert operation in DynamoDB.
func (i *Item) processBatchUpsert(ctx context.Context, batch map[string][]types.WriteRequest) error {
	var retries int
	for {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		}

		result, err := i.c.client.BatchWriteItem(ctx, input)
		if err != nil {
			return dynamoError().method(opBatchUpsert).message(err.Error())
		}

		if len(result.UnprocessedItems) == 0 || retries >= maxRetriesUpsert {
			break
		}

		// Retry unprocessed items
		batch = result.UnprocessedItems
		retries++
	}

	// Sleep for 1 second to avoid throttling
	time.Sleep(time.Second * 1)
	return nil
}
