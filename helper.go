package dygo

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// createAttributeValue creates a types.AttributeValue based on the input type.
func createAttributeValue(key interface{}) types.AttributeValue {
	switch k := key.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: k}
	case int:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(k)}
	}
	return nil
}

// marshalMapUsingJSONTags marshals the given input interface into a map of DynamoDB attribute values,
// using JSON tags for serialization. It first converts the input into JSON data, then unmarshals
// the JSON data into a map[string]interface{}. Finally, it marshals the map into a map[string]types.AttributeValue
// using the attributevalue.MarshalMap function from the AWS SDK for Go.
//
// The input interface should have JSON tags defined on its fields to specify the attribute names in DynamoDB.
// The resulting map will have the attribute names as keys and the corresponding attribute values as values.
//
// If any error occurs during the marshaling process, it will be returned along with a nil map.
func marshalMapUsingJSONTags(i interface{}) (map[string]types.AttributeValue, error) {
	jsonData, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	var jsonMap map[string]interface{}
	err = json.Unmarshal(jsonData, &jsonMap)
	if err != nil {
		return nil, err
	}

	return attributevalue.MarshalMap(jsonMap)
}

// validate validates the given key and value.
func (i *Item) validate(key string, value any) error {
	switch key {
	case "PK":
		return i.validatePartitionKey(value)
	case "SK":
		return i.validateSortKey(value)
	case "Projection":
		return i.validateProjecion(value)
	case "GSI":
		return i.validateGSI(value)
	case "FilterAnd":
		return i.validateFilterAnd(value)
	case "FilterOr":
		return i.validateFilterOr(value)
	case "TableName":
		return i.validateTableName(value)
	}
	return nil
}

// validateTableName checks if the provided table name is empty.
func (i *Item) validateTableName(value any) error {
	if value == "" {
		return dynamoError().method("TableName").message("table name can't be empty")
	}
	return nil
}

// validateFilterOr validates the filter OR condition for an Item.
func (i *Item) validateFilterOr(value any) error {
	return dynamoError().method("FilterAnd").message("invalid filter OR condition")
}

// validateFilterAnd validates the filter AND condition for an Item.
func (i *Item) validateFilterAnd(value any) error {
	return dynamoError().method("FilterAnd").message("invalid filter AND condition")
}

// validateGSI validates the Global Secondary Index (GSI) value.
func (i *Item) validateGSI(value any) error {
	found := false
	for _, gsi := range i.c.gsis {
		if gsi.indexName == i.indexName {
			found = true
		}
	}
	if !found {
		return dynamoError().method("GSI").message("invalid GSI name")
	}
	return nil
}

// validatePartitionKey checks if the provided partition key value is empty.
func (i *Item) validatePartitionKey(value any) error {
	if value == "" {
		return dynamoError().method("PK").message("partition key can't be empty")
	}
	return nil
}

// validateSortKey validates the sort key value for an Item.
func (i *Item) validateSortKey(value any) error {
	msg := ""
	if i.c.sortKey == "" {
		msg = "sort key is not required"
	}
	if i.c.sortKey != "" && value == "" {
		msg = "sort key can't be empty"
	}
	if msg != "" {
		return dynamoError().method("SK").message(msg)
	}
	return nil
}

// validateProjecion validates the projection value for an Item.
func (i *Item) validateProjecion(value any) error {
	msg := ""
	if value == "" {
		msg = "projection can't be empty"
		return dynamoError().method("Projection").message(msg)
	}
	return nil
}

// isGetItemValid checks if the provided item is valid for a GetItem operation.
func (i *Item) isGetItemValid() error {
	switch {
	case i.c.partitionKey == "":
		return errors.New("partition key name is empty")
	case i.c.tableName == "":
		return errors.New("table name is empty")
	case i.key == nil:
		return errors.New("key is empty")
	case i.key != nil:
		if _, ok := i.key[i.c.partitionKey]; !ok {
			return errors.New("partition key is empty")
		}
		if i.c.sortKey != "" {
			if _, ok := i.key[i.c.sortKey]; !ok {
				return errors.New("sort key is empty")
			}
		}
		if i.c.sortKey == "" {
			if len(i.key) > 1 {
				return errors.New("too many keys")
			}
		}
	case i.useGSI:
		return errors.New("couldn't use gsi with get item")
	}
	return nil
}

// stringExists checks if a given string exists in a slice of strings.
func stringExists(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func getSplittedKey(key string, separator string) string {
	if separator != "" {
		return strings.Split(key, separator)[0]
	}
	return key
}
