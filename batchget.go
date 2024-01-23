package dygo

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

const opBatchGet = "BatchGet"

// BatchGetItem retrieves multiple items from the DynamoDB table in parallel using multiple threads.
// It takes a context and the number of threads as input parameters.
// It returns a slice of maps, where each map represents an item retrieved from the table, along with any error encountered.
//
// Example:
//
//	item := new(Item)
//		for _, gId := range gIds {
//			db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
//		}
//
//		output, err := item.BatchGetItem(context.Background(), 10)
func (i *Item) BatchGetItem(ctx context.Context, threadCount int) ([]map[string]types.AttributeValue, error) {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(threadCount)

	var mu sync.Mutex
	output := make([]map[string]types.AttributeValue, 0)

	for _, batch := range i.batchData.batchGet {
		batch := batch
		g.Go(func() error {
			return i.fetchBatch(ctx, batch, &output, &mu)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, DynamoError().Method(opBatchGet).Message(err.Error())
	}

	return output, nil
}

// BatchGetAuthorizedItem fetches multiple items from DynamoDB in parallel, with authorization.
// It uses a specified number of goroutines to perform the batch get operation concurrently.
// The fetched items are stored in the Output struct and returned as a result.
// Use Unmarshal() to unmarshal the results into the provided 'out' object.
//
// Example:
//
//	func (d *dataItem) Authorize(ctx context.Context) error {
//		return nil
//	}
//
//	item := new(Item)
//
//	for _, gId := range gIds {
//		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
//	}
//
//	var data []dataItem
//	err = item.BatchGetAuthorizedItem(context.Background(), 10).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (i *Item) BatchGetAuthorizedItem(ctx context.Context, threadCount int) *Output {
	result := NewOutput(i, ctx)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(threadCount)

	var mu sync.Mutex
	output := make([]map[string]types.AttributeValue, 0)

	for _, batch := range i.batchData.batchGet {
		batch := batch
		g.Go(func() error {
			return i.fetchBatch(ctx, batch, &output, &mu)
		})
	}

	if err := g.Wait(); err != nil {
		result.item.err = DynamoError().Method(opBatchGet).Message(err.Error())
	}
	result.Results = append(result.Results, output...)
	return result
}

// fetchBatch fetches items in batches from DynamoDB using BatchGetItem API.
func (i *Item) fetchBatch(ctx context.Context, batch map[string]types.KeysAndAttributes, output *[]map[string]types.AttributeValue, mu *sync.Mutex) error {
	input := &dynamodb.BatchGetItemInput{
		RequestItems: batch,
	}

	paginator := dynamodb.NewBatchGetItemPaginator(i.c.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return DynamoError().Method(opBatchGet).Message(err.Error())
		}

		mu.Lock()
		for _, items := range page.Responses {
			*output = append(*output, items...)
		}
		mu.Unlock()
	}
	return nil
}
