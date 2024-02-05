package dygo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// batchGetItemPaginatorOptions is the paginator options for BatchGetItem
type batchGetItemPaginatorOptions struct {
	// Set to true if pagination should stop if the service returns a pagination token
	// that matches the most recent token provided to the service.
	stopOnDuplicateToken bool
}

// batchGetItemPaginator is a paginator for BatchGetItem
type batchGetItemPaginator struct {
	options      batchGetItemPaginatorOptions
	client       batchGetItemAPIClient
	params       *dynamodb.BatchGetItemInput
	firstPage    bool
	requestItems map[string]types.KeysAndAttributes
	isTruncated  bool
}

// batchGetItemAPIClient is a client that implements the BatchGetItem operation.
type batchGetItemAPIClient interface {
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

// newBatchGetItemPaginator returns a new batchGetItemPaginator
func newBatchGetItemPaginator(client batchGetItemAPIClient, params *dynamodb.BatchGetItemInput, optFns ...func(*batchGetItemPaginatorOptions)) *batchGetItemPaginator {
	if params == nil {
		params = &dynamodb.BatchGetItemInput{}
	}

	options := batchGetItemPaginatorOptions{}

	for _, fn := range optFns {
		fn(&options)
	}

	return &batchGetItemPaginator{
		options:      options,
		client:       client,
		params:       params,
		firstPage:    true,
		requestItems: params.RequestItems,
	}
}

// hasMorePages returns a boolean indicating whether more pages are available
func (p *batchGetItemPaginator) hasMorePages() bool {
	return p.firstPage || p.isTruncated
}

// nextPage retrieves the next BatchGetItem page.
func (p *batchGetItemPaginator) nextPage(ctx context.Context, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if !p.hasMorePages() {
		return nil, fmt.Errorf("no more pages available")
	}

	params := *p.params
	params.RequestItems = p.requestItems

	result, err := p.client.BatchGetItem(ctx, &params, optFns...)
	if err != nil {
		return nil, err
	}
	p.firstPage = false

	prevToken := p.requestItems
	p.isTruncated = len(result.UnprocessedKeys) != 0
	p.requestItems = nil
	if p.isTruncated {
		p.requestItems = result.UnprocessedKeys
	}

	if p.options.stopOnDuplicateToken &&
		prevToken != nil &&
		p.requestItems != nil &&
		deepEqual(prevToken, p.requestItems) {
		p.isTruncated = false
	}

	return result, nil
}

// deepEqual returns if the two values are deeply equal like reflect.DeepEqual.
// In addition to this, this method will also dereference the input values if
// possible so the DeepEqual performed will not fail if one parameter is a
// pointer and the other is not.
//
// deepEqual will not perform indirection of nested values of the input parameters.
func deepEqual(a, b interface{}) bool {
	ra := reflect.Indirect(reflect.ValueOf(a))
	rb := reflect.Indirect(reflect.ValueOf(b))

	if raValid, rbValid := ra.IsValid(), rb.IsValid(); !raValid && !rbValid {
		// If the elements are both nil, and of the same type the are equal
		// If they are of different types they are not equal
		return reflect.TypeOf(a) == reflect.TypeOf(b)
	} else if raValid != rbValid {
		// Both values must be valid to be equal
		return false
	}

	// Special casing for strings as typed enumerations are string aliases
	// but are not deep equal.
	if ra.Kind() == reflect.String && rb.Kind() == reflect.String {
		return ra.String() == rb.String()
	}

	return reflect.DeepEqual(ra.Interface(), rb.Interface())
}
