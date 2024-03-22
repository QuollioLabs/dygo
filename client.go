// dygo is a Go package that provides a simple and clean interface for interacting with DynamoDB.
package dygo

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/logging"
)

type Option func(*Client) error

// Client is the main struct for the dygo package. It contains DynamoDB client, table name, partition key,
// sort key, and other configuration options.
type Client struct {
	client       *dynamodb.Client
	region       string
	tableName    string
	partitionKey string
	sortKey      string
	gsis         []gsi
	endpoint     string
	maxRetry     int
	logger       *log.Logger
	keySeparator string
}

// GSI is a struct that represents a Global Secondary Index (GSI) for the client.
type gsi struct {
	indexName    string
	partitionKey string
	sortKey      string
}

// WithTableName is a mandatory option function that sets the table name for the client.
// It takes a tableName string as a parameter and returns an error.
func WithTableName(tableName string) Option {
	return func(c *Client) error {
		c.tableName = tableName
		return nil
	}
}

// WithPartitionKey is a mandatory option function that sets the partition key for the client.
// It takes a string parameter 'key' and returns an error.
func WithPartitionKey(key string) Option {
	return func(c *Client) error {
		c.partitionKey = key
		return nil
	}
}

// WithSortKey is an optional option function that sets the sort key for the client.
// Example:
//
//	client := NewClient(WithSortKey("sk"))
//	// This sets the sort key to "sk" for the client.
func WithSortKey(key string) Option {
	return func(c *Client) error {
		c.sortKey = key
		return nil
	}
}

// WithKeySeparator sets the key separator for the client.
// The key separator is used to separate different parts of partition key in the client.
func WithKeySeparator(separator string) Option {
	return func(c *Client) error {
		c.keySeparator = separator
		return nil
	}
}

// WithGSI is an optional option function that adds a Global Secondary Index (GSI) to the client.
// It takes the index name, partition key, and sort key as parameters.
// If a GSI with the same index name already exists, it returns an error.
// Otherwise, it adds the GSI to the client and returns nil.
func WithGSI(indexName, partitionKey, sortKey string) Option {
	return func(c *Client) error {
		for _, v := range c.gsis {
			if v.indexName == indexName {
				return errors.New("duplicate gsi index name")
			}
		}
		c.gsis = append(c.gsis, gsi{indexName, partitionKey, sortKey})
		return nil
	}
}

// WithRegion is a mandatory option function that sets the region for the client.
// It takes a string parameter representing the region and returns an error.
// The region is used to configure the client for a specific geographic region.
func WithRegion(region string) Option {
	return func(c *Client) error {
		c.region = region
		return nil
	}
}

// WithLogger is an optional option function that sets the logger for the client.
// It takes a *log.Logger as a parameter and returns an error.
// The logger will be used to log client operations and errors.
func WithLogger(l *log.Logger) Option {
	return func(c *Client) error {
		c.logger = l
		return nil
	}
}

// WithEndpoint is an optional option function that sets the endpoint for the client.
// It takes an endpoint string as a parameter and returns an error.
func WithEndpoint(endpoint string) Option {
	return func(c *Client) error {
		c.endpoint = endpoint
		return nil
	}
}

// WithRetry is an optional option function that sets the maximum number of retries for a client.
// It takes an integer count as a parameter and returns an error.
// The count parameter specifies the maximum number of retries allowed.
// If the count is not provided, it defaults to 5.
func WithRetry(count int) Option {
	return func(c *Client) error {
		c.maxRetry = count
		return nil
	}
}

// Define a custom logger that satisfies the log.Logger interface.
type customLogger struct {
	logger *log.Logger
}

// TODO: fix me
func (l customLogger) Logf(classification logging.Classification, format string, v ...any) {
	// Here you can format the log message as you like and
	// write it to the underlying logger.
	l.logger.Printf("%v:%v", classification, v)
}

// loadDBConfigOptions returns a slice of functions that configure the options for loading the database configuration.
// The returned functions can be used with the `config.LoadOptions` struct to customize the configuration options.
// The `c` parameter is the client used to interact with the database.
func loadDBConfigOptions(c Client) []func(*config.LoadOptions) error {
	var options []func(*config.LoadOptions) error
	options = append(options, config.WithRegion(c.region))

	if c.logger != nil {
		cl := customLogger{logger: c.logger}
		options = append(options, config.WithLogger(cl))
		options = append(options, config.WithClientLogMode(aws.LogRetries|aws.LogRequest))
	}

	count := c.maxRetry
	if c.maxRetry <= 0 {
		count = 5
	}
	options = append(options, config.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), count)
	}))
	if c.endpoint != "" {
		options = append(options, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...any) (aws.Endpoint, error) {
				if service == dynamodb.ServiceID {
					return aws.Endpoint{
						URL:           c.endpoint,
						SigningRegion: c.region,
					}, nil
				}
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			},
		)))
	}
	return options
}

// NewClient creates a new instance of the Client struct with the provided options.
// It initializes the DynamoDB client using provided configuration.
// Returns the created Client instance or an error if any.
//
// Example:
//
//	dbEndpoint := "http://localhost:8000"
//	dbClient, err := NewClient(
//		WithTableName("test-table-1"),
//		WithRegion("ap-northeast-1"),
//		WithPartitionKey("_partition_key"),
//		WithSortKey("_sort_key"),
//		WithGSI("gsi-name", "_entity_type", "_sort_key"),
//		WithEndpoint(dbEndpoint),
//	)
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	options := loadDBConfigOptions(*c)
	cfg, err := config.LoadDefaultConfig(context.TODO(), options...)
	if err != nil {
		return nil, err
	}
	c.client = dynamodb.NewFromConfig(cfg)
	e := c.validate()
	if e != nil {
		return nil, e
	}
	return c, nil
}

// validate checks if the required fields of the Client struct are set.
// If any required field is missing, it returns an Error with the corresponding error message.
// If all required fields are set, it returns nil.
func (c *Client) validate() *dError {
	var msg string
	switch {
	case c.partitionKey == "":
		msg = errMissingPartitionKey
	case c.region == "":
		msg = errMissingRegion
	case c.client == nil:
		msg = errMissingClient
	}
	if msg != "" {
		return dynamoError().method("NewClient").message(msg)
	}
	return nil
}

// Table sets the name of the table for the current query.
// It returns the modified Client instance.
func (c *Client) Table(value string) *Client {
	c.tableName = value
	return c
}

// PK sets the specified value as the partition key value in current query.
func (c *Client) PK(value any) *Item {
	return c.partition(c.partitionKey, value)
}

// GSI sets Global Secondary Index (GSI) for quiry.
// It takes the indexName string, partitionKeyValue any, and f SortKeyFunc as parameters.
// The indexName specifies the name of the GSI.
// The partitionKeyValue specifies the value of the partition key for the GSI query.
// The f SortKeyFunc is a function that defines the sort key for the GSI query.
// It can be Equal, BeginsWith, Between, LessThan, LessThanEqual, GreaterThan, GreaterThanEqual or nil
// It returns an Item object that can be used to perform operations on the GSI.
//
// Example:
//
//	 err = db.
//		GSI("gsi-name", "room", dygo.Equal("current")).
//		Query(context.Background()).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (c *Client) GSI(indexName string, partitionKeyValue any, f SortKeyFunc) *Item {
	return c.secondaryIndex(indexName, partitionKeyValue, f)
}

// SK sets the provided sort key value along with SortKeyFunc.
// The SortKeyFunc is used to determine the sorting order of the Item.
// Possible values for SortKeyFunc are Equal, BeginsWith, Between, LessThan, LessThanEqual, GreaterThan, GreaterThanEqual.
//
// Example:
//
//	 err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		GetItem(context.Background(), &data)
func (i *Item) SK(f SortKeyFunc) *Item {
	return i.sort(i.c.sortKey, f)
}

// ScanIndexForward sets the flag indicating whether the scan operation should be performed in ascending order.
// If value is true, the scan will be performed in ascending order. If value is false, the scan will be performed in descending order.
//
// Example:
//
//	 err = db.
//		GSI("gsi-name", "room", dygo.Equal("current")).
//		ScanIndexForward(true).
//		Query(context.Background()).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (i *Item) ScanIndexForward(value bool) *Item {
	i.pagination.desc = value
	return i
}

// Item returns a new instance of the Item struct, initialized with the provided item and client.
// item must implement method : Validate() error
//
// Example:
//
// If passing employee struct, it should implement Validate() error
//
//	func (e employee) Validate() error {
//		return nil
//	}
//	emp := employee{}
//	err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		GetItem(context.Background(), &emp)
func (c *Client) Item(item ItemData) *Item {
	i := &Item{
		c:    c,
		item: item,
	}
	i.err = i.validate("TableName", c.tableName)
	return i
}

// ItemRaw returns a new instance of the Item struct, initialized with the provided raw item and client.
// It does not require the item to implement the Validate() method.
//
// Example:
//
//	newItem := new(Item)
//	for _, item := range items {
//		db.ItemRaw(item).AddBatchUpsertRawItem(newItem)
//	}
//	// here items is of type []map[string]types.AttributeValue
//
//	err = newItem.BatchUpsertItem(context.Background(), 10)
//	if err != nil {
//		log.Fatal(err)
//	}
func (c *Client) ItemRaw(items map[string]types.AttributeValue) *Item {
	i := &Item{
		c: c,
	}
	i.batchData.batchPutRaw = items
	i.err = i.validate("TableName", c.tableName)
	return i
}

// UpdateItem returns a new instance of the Item struct, initialized with the provided item and client.
// It is used to update an existing item in the table.
//
// Example:
//
//	newItem := new(Item)
//	for _, item := range items {
//		db.UpdateItemRaw(item).AddUpdateRawItem(newItem)
//	}
//
//	err = newItem.Update(context.Background(), 5)
func (c *Client) UpdateItemRaw(item map[string]any) *Item {
	i := &Item{
		c: c,
	}

	k := make(map[string]types.AttributeValue)
	k[c.partitionKey] = createAttributeValue(item[c.partitionKey])
	delete(item, c.partitionKey)
	if c.sortKey != "" {
		k[c.sortKey] = createAttributeValue(item[c.sortKey])
		delete(item, c.sortKey)
	}

	i.batchData.updateItems = append(i.batchData.updateItems, updateItem{updateItem: item, key: k})
	i.err = i.validate("TableName", c.tableName)
	return i
}

// Project sets the projection for the item.
// It takes a variadic parameter `value` which represents the projection fields.
//
// Example:
//
//	 err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		Project("_partition_key", "_entity_type", "_sort_key").
//		GetItem(context.Background(), &data)
func (i *Item) Project(value ...string) *Item {
	return i.setProjection(value)
}

// Filter applies a filter function to the specified attribute of the item.
// Possible values for FilterFunc are KeyEqual, KeyNotEqual, KeyBeginsWith, KeyBetween, KeyLessThan, KeyLessThanEqual, KeyGreaterThan, KeyGreaterThanEqual, KeyContains, KeyNotNull, KeyNull, KeyIn.
//
// Example:
//
//	 err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		Filter("physical_name", dygo.KeyBeginsWith("name_test_")).
//		GetItem(context.Background(), &data)
func (i *Item) Filter(attributeName string, filterFunc FilterFunc) *Item {
	return i.buildFilter(attributeName, filterFunc)
}

// AndFilter applies an additional logical AND filter to the existing filter using the specified attribute name and filter function.
// It should be used after the Filter function.
//
// Example:
//
//	 err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		Filter("physical_name", dygo.KeyBeginsWith("name_test_")).
//		AndFilter("logical_name", dygo.KeyBeginsWith("name_test_")).
//		GetItem(context.Background(), &data)
func (i *Item) AndFilter(attributeName string, filterFunc FilterFunc) *Item {
	return i.buildAndFilter(attributeName, filterFunc)
}

// OrFilter method is used to chain multiple filters together using the OR operator.
// It should be used after the Filter function.
//
// Example:
//
//	 err = db.
//		PK("pk").
//		SK(dygo.Equal("sk")).
//		Filter("physical_name", dygo.KeyBeginsWith("name_test_")).
//		OrFilter("logical_name", dygo.KeyBeginsWith("name_test_")).
//		GetItem(context.Background(), &data)
func (i *Item) OrFilter(attributeName string, filterFunc FilterFunc) *Item {
	return i.buildOrFilter(attributeName, filterFunc)
}

// AddBatchGetItem adds a new item to the batch get item request.
// If omitEmptyKeys is true empty keys will not be added to BatchGetItem.
// If omitEmptyKeys is false empty keys also be added to BatchGetItem.
//
// Example:
//
//	 item := new(Item)
//	 for _, gId := range gIds {
//		 db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
//	 }
//	 output, err := item.BatchGetItem(context.Background(), 10)
func (i *Item) AddBatchGetItem(newItem *Item, omitEmptyKeys bool) {
	if omitEmptyKeys && i.isPartitionKeyEmpty() {
		return
	} else {
		if i.err != nil {
			newItem.err = i.err
			return
		}
	}
	i.fillItem(newItem)
	newItem.addBatchGetItem()
}

// isPartitionKeyEmpty checks if the partition key of the item is empty.
func (i *Item) isPartitionKeyEmpty() bool {
	if pk, ok := i.key[i.c.partitionKey]; ok {
		switch v := pk.(type) {
		case *types.AttributeValueMemberS:
			if v.Value == "" {
				return true
			}
		}
	}
	return false
}

// AddBatchDeleteItem adds a new item to the batch delete list.
//
// Example:
//
//	 item := new(Item)
//	 for _, gId := range gIds {
//		 db.PK(gId).SK(Equal(SK)).AddBatchDeleteItem(item)
//	 }
//	 err = item.BatchDeleteItem(context.Background(), 10)
func (i *Item) AddBatchDeleteItem(newItem *Item) {
	if i.err != nil {
		newItem.err = i.err
		return
	}
	i.fillItem(newItem)
	newItem.addBatchDeleteItem()
}

// AddBatchUpsertItem adds a new item to the batch upsert operation.
//
// Example:
//
//	 item := new(Item)
//	 for _, gId := range gIds {
//		 db.PK(gId).SK(Equal(SK)).AddBatchUpsertItem(item)
//	 }
//	 err = item.BatchUpsertItem(context.Background(), 10)
func (i *Item) AddBatchUpsertItem(newItem *Item) {
	err := i.item.Validate()
	if err != nil {
		newItem.err = dynamoError().method("opValidate").message(err.Error())
		return
	}
	i.fillItem(newItem)
	newItem.addBatchUpsertItem(false)
}

// AddUpdateRawItem adds a new raw item (types.AttributeValue) to the update operation.
//
// Example:
//
//	newItem := new(Item)
//	for _, item := range items {
//		db.UpdateItemRaw(item).AddUpdateRawItem(newItem)
//	}
//
//	err = newItem.Update(context.Background(), 5)
func (i *Item) AddUpdateRawItem(newItem *Item) {
	i.fillItem(newItem)
}

// AddBatchUpsertRawItem adds a new raw item (types.AttributeValue) to the batch upsert operation.
//
// Example:
//
//	newItem := new(Item)
//	for _, item := range items {
//		db.ItemRaw(item).AddBatchUpsertRawItem(newItem)
//	}
//	// here items is of type []map[string]types.AttributeValue
//
//	err = newItem.BatchUpsertItem(context.Background(), 10)
//	if err != nil {
//		log.Fatal(err)
//	}
func (i *Item) AddBatchUpsertRawItem(newItem *Item) {
	i.fillItem(newItem)
	newItem.addBatchUpsertItem(true)
}

// fillItem fills the fields of the given newItem with the values from the current Item.
func (i *Item) fillItem(newItem *Item) {
	newItem.c = i.c
	newItem.key = i.key
	newItem.keyCondition = i.keyCondition
	newItem.filter = i.filter
	newItem.projection = i.projection
	newItem.item = i.item
	newItem.useGSI = i.useGSI
	newItem.pagination = i.pagination
	newItem.indexName = i.indexName
	newItem.batchData.batchPutRaw = i.batchData.batchPutRaw
	newItem.batchData.updateItems = append(newItem.batchData.updateItems, i.batchData.updateItems...)
	if newItem.err == nil {
		newItem.err = i.err
	}
}
