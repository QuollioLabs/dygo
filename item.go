package dygo

import (
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Item represents a DynamoDB item and provides methods to perform operations on it.
type Item struct {
	c            *Client
	indexName    string
	projection   string
	useGSI       bool
	item         ItemData
	err          error
	batchData    keys
	pagination   pagination
	filter       expression.ConditionBuilder
	key          map[string]types.AttributeValue // required only for GetItem/DeleteItem
	keyCondition expression.KeyConditionBuilder
}

// ItemData is an interface that represents a DynamoDB item. Each data item must implement this interface.
// It is used to validate the item using user defined Validate function before performing any operations on it.
type ItemData interface {
	Validate() error
}

type keys struct {
	batchGet    map[int]map[string]types.KeysAndAttributes
	batchPut    map[int]map[string][]types.WriteRequest
	batchDelete map[int]map[string][]types.WriteRequest
}

type pagination struct {
	lastEvaluatedKey map[string]types.AttributeValue
	limit            int32
	desc             bool
}

const (
	getBatchSize   = 100
	writeBatchSize = 25
	none           = ""
)

// partition creates a new Item with the specified key and value, and associates it with the Client.
// It returns a pointer to the created Item.
func (c *Client) partition(key string, value any) *Item {
	k := make(map[string]types.AttributeValue)
	k[key] = createAttributeValue(value)
	item := Item{
		c:            c,
		key:          k,
		keyCondition: expression.KeyEqual(expression.Key(key), expression.Value(value.(string))),
	}
	item.err = item.validate("TableName", c.tableName)
	if item.err == nil {
		item.err = item.validate("PK", value)
	}
	return &item
}

// sort sets sort key value.
func (i *Item) sort(key string, f SortKeyFunc) *Item {
	keyCondition, sortKeyValue := f(key)
	if i.err == nil {
		i.err = i.validate("SK", sortKeyValue)
	}
	i.key[key] = createAttributeValue(sortKeyValue)
	i.keyCondition.And(keyCondition)
	return i
}

// buildFilter builds a filter for the Item based on the provided attributeName and FilterFunc.
func (i *Item) buildFilter(attributeName string, f FilterFunc) *Item {
	condition := f(attributeName)
	i.filter = condition
	return i
}

// buildAndFilter builds and filters the Item based on the provided attributeName and FilterFunc.
// It returns the modified Item after applying the filter.
func (i *Item) buildAndFilter(attributeName string, f FilterFunc) *Item {
	if !i.filter.IsSet() && i.err == nil {
		i.err = i.validate("FilterAnd", none)
		return i
	}
	condition := f(attributeName)
	i.filter = i.filter.And(condition)
	return i
}

// buildOrFilter builds and adds an OR filter condition to the Item's filter.
func (i *Item) buildOrFilter(attributeName string, f FilterFunc) *Item {
	if !i.filter.IsSet() && i.err == nil {
		i.err = i.validate("FilterOr", none)
		return i
	}
	condition := f(attributeName)
	i.filter = i.filter.Or(condition)
	return i
}

// setProjection sets the projection value for the Item.
func (i *Item) setProjection(value []string) *Item {
	i.projection = strings.Join(value, ",")
	if i.err == nil {
		i.err = i.validate("Projection", i.projection)
	}
	return i
}

// LastEvaluatedKey sets the last evaluated key for pagination.
//
// Example:
//
//	err = db.
//		GSI("gsi-name", "room", Equal("current")).
//		Limit(2).
//		LastEvaluatedKey(lek).
//		Query(context.Background()).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (i *Item) LastEvaluatedKey(keys map[string]any) *Item {
	i.pagination.lastEvaluatedKey = make(map[string]types.AttributeValue)
	for key, value := range keys {
		i.pagination.lastEvaluatedKey[key] = createAttributeValue(value)
	}
	return i
}

// Limit sets the maximum number of items to be returned in the pagination.
//
// Example:
//
//	err = db.
//		GSI("gsi-name", "room", Equal("current")).
//		Limit(2).
//		LastEvaluatedKey(lek).
//		Query(context.Background()).
//		Unmarshal(&data, []string{"room"}).
//		Run()
func (i *Item) Limit(itemCount int) *Item {
	i.pagination.limit = int32(itemCount)
	return i
}

// secondaryIndex returns a new Item instance with the specified secondary index details.
// It takes the indexName string, partitionKeyValue any, and f SortKeyFunc as parameters.
func (c *Client) secondaryIndex(indexName string, partitionKeyValue any, f SortKeyFunc) *Item {
	item := &Item{
		c: c,
	}
	for _, sIndex := range c.gsis {
		if sIndex.indexName == indexName {
			keyCondition := expression.KeyEqual(expression.Key(sIndex.partitionKey), expression.Value(partitionKeyValue.(string)))
			if f != nil {
				sortKeyCond, _ := f(sIndex.sortKey)
				keyCondition = keyCondition.And(sortKeyCond)
			}
			item.indexName = indexName
			item.keyCondition = keyCondition
			item.useGSI = true
		}
	}
	item.err = item.validate("TableName", c.tableName)
	if item.err == nil {
		item.err = item.validate("GSI", none)
	}
	return item
}

// addBatchGetItem adds the current item to the batch request for GetItem operation.
func (i *Item) addBatchGetItem() {
	if i.batchData.batchGet == nil {
		i.batchData.batchGet = make(map[int]map[string]types.KeysAndAttributes)
	}
	batchIndex := i.findBatchIndex()
	// Check if there is an entry for the table in the current batch. If not, create it.
	if _, ok := i.batchData.batchGet[batchIndex][i.c.tableName]; !ok {
		i.batchData.batchGet[batchIndex][i.c.tableName] = types.KeysAndAttributes{Keys: []map[string]types.AttributeValue{}}
	}
	batchIndex = i.findBatchIndexIfBatchFull(batchIndex)
	// Only add the key if it's not already in the batch
	if !i.keyExists(batchIndex, i.key) {
		// Extract the KeysAndAttributes struct, modify it, and put it back in the map.
		keysAndAttributes := i.batchData.batchGet[batchIndex][i.c.tableName]
		keysAndAttributes.Keys = append(keysAndAttributes.Keys, i.key)
		i.batchData.batchGet[batchIndex][i.c.tableName] = keysAndAttributes
	}
}

// findBatchIndex returns the index of the current batch in the Item's batchData.
func (i *Item) findBatchIndex() int {
	batchIndex := len(i.batchData.batchGet) - 1
	if batchIndex < 0 {
		batchIndex++
		i.batchData.batchGet[batchIndex] = make(map[string]types.KeysAndAttributes)
	}
	return batchIndex
}

// findBatchIndexIfBatchFull finds the index of the batch and returns the updated batch index if the batch is full.
func (i *Item) findBatchIndexIfBatchFull(batchIndex int) int {
	batchIndexLength := 0
	for _, keys := range i.batchData.batchGet[batchIndex] {
		batchIndexLength += len(keys.Keys)
	}
	if batchIndexLength >= getBatchSize {
		batchIndex++
		i.batchData.batchGet[batchIndex] = make(map[string]types.KeysAndAttributes)
	}
	return batchIndex
}

func (i *Item) keyExists(batchIndex int, key map[string]types.AttributeValue) bool {
	for _, existingKey := range i.batchData.batchGet[batchIndex][i.c.tableName].Keys {
		if reflect.DeepEqual(existingKey, key) {
			return true
		}
	}
	return false
}

// addBatchDeleteItem adds a delete request for the current item to the batch delete operation.
func (i *Item) addBatchDeleteItem() {
	if i.batchData.batchDelete == nil {
		i.batchData.batchDelete = make(map[int]map[string][]types.WriteRequest)
	}
	batchIndex := i.findBatchDeleteIndex()
	if _, ok := i.batchData.batchDelete[batchIndex][i.c.tableName]; !ok {
		i.batchData.batchDelete[batchIndex][i.c.tableName] = []types.WriteRequest{}
	}
	batchIndex = i.findBatchDeleteIndexIfBatchFull(batchIndex)
	i.batchData.batchDelete[batchIndex][i.c.tableName] = append(i.batchData.batchDelete[batchIndex][i.c.tableName], types.WriteRequest{
		DeleteRequest: &types.DeleteRequest{
			Key: i.key,
		},
	})
}

// findBatchDeleteIndex returns the index of the batch delete operation in the Item's batchData.
func (i *Item) findBatchDeleteIndex() int {
	batchIndex := len(i.batchData.batchDelete) - 1
	if batchIndex < 0 {
		batchIndex++
		i.batchData.batchDelete[batchIndex] = make(map[string][]types.WriteRequest)
	}
	return batchIndex
}

// findBatchDeleteIndexIfBatchFull finds the index of the batch to delete items from, and if the batch is full, it creates a new batch.
func (i *Item) findBatchDeleteIndexIfBatchFull(batchIndex int) int {
	batchIndexLength := 0
	for _, keys := range i.batchData.batchDelete[batchIndex] {
		batchIndexLength += len(keys)
	}
	if batchIndexLength >= writeBatchSize {
		batchIndex++
		i.batchData.batchDelete[batchIndex] = make(map[string][]types.WriteRequest)
	}
	return batchIndex
}

// addBatchUpsertItem adds the current item to the batch put operation.
func (i *Item) addBatchUpsertItem() {
	if i.batchData.batchPut == nil {
		i.batchData.batchPut = make(map[int]map[string][]types.WriteRequest)
	}
	batchIndex := i.findBatchPutIndex()
	if _, ok := i.batchData.batchPut[batchIndex][i.c.tableName]; !ok {
		i.batchData.batchPut[batchIndex][i.c.tableName] = []types.WriteRequest{}
	}
	batchIndex = i.findBatchPutIndexIfBatchFull(batchIndex)
	// TODO: log error
	itemJson, _ := marshalMapUsingJSONTags(i.item)
	i.batchData.batchPut[batchIndex][i.c.tableName] = append(i.batchData.batchPut[batchIndex][i.c.tableName], types.WriteRequest{
		PutRequest: &types.PutRequest{
			Item: itemJson,
		},
	})
}

// findBatchPutIndex returns the index of the current batch in the Item's batchPut slice.
func (i *Item) findBatchPutIndex() int {
	batchIndex := len(i.batchData.batchPut) - 1
	if batchIndex < 0 {
		batchIndex++
		i.batchData.batchPut[batchIndex] = make(map[string][]types.WriteRequest)
	}
	return batchIndex
}

// findBatchPutIndexIfBatchFull finds the index of the batch in the Item's if current batch is full.
func (i *Item) findBatchPutIndexIfBatchFull(batchIndex int) int {
	batchIndexLength := 0
	for _, keys := range i.batchData.batchPut[batchIndex] {
		batchIndexLength += len(keys)
	}
	if batchIndexLength >= writeBatchSize {
		batchIndex++
		i.batchData.batchPut[batchIndex] = make(map[string][]types.WriteRequest)
	}
	return batchIndex
}
