package dygo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

type mockDataItem struct {
	ID                string `dynamodbav:"id"`
	CustomEntityType  string `dynamodbav:"custom_entity_type"`
	DefaultEntityType string `dynamodbav:"_object_type"`
	OtherField        string `dynamodbav:"other_field"`
	AuthorizeErr      error  `dynamodbav:"-"`
}

type mockDataItems []mockDataItem

func (items *mockDataItems) Authorize(ctx context.Context) error {
	// Simulate authorization for each item
	for _, item := range *items {
		if item.AuthorizeErr != nil {
			return item.AuthorizeErr
		}
	}
	return nil
}

func TestOutput_Unmarshal(t *testing.T) {
	tests := []struct {
		name                      string
		results                   []map[string]types.AttributeValue
		entityTypes               []string
		bypassAuth                bool
		customObjectTypeAttribute string
		expectedData              mockDataItems
		expectedErr               string
		separator                 string
	}{
		{
			name: "Unmarshal with custom object type attribute",
			results: []map[string]types.AttributeValue{
				{
					"id":                 &types.AttributeValueMemberS{Value: "6"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "vehicle"},
					"other_field":        &types.AttributeValueMemberS{Value: "car"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
				{
					"id":                 &types.AttributeValueMemberS{Value: "7"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "vehicle"},
					"other_field":        &types.AttributeValueMemberS{Value: "bike"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
				{
					"id":                 &types.AttributeValueMemberS{Value: "8"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "appliance"},
					"other_field":        &types.AttributeValueMemberS{Value: "fridge"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
			},
			entityTypes:               []string{"vehicle"},
			customObjectTypeAttribute: "custom_entity_type",
			expectedData: mockDataItems{
				{ID: "6", CustomEntityType: "vehicle", OtherField: "car", DefaultEntityType: "prefix#UUID"},
				{ID: "7", CustomEntityType: "vehicle", OtherField: "bike", DefaultEntityType: "prefix#UUID"},
			},
			expectedErr: "",
		},
		{
			name: "Unmarshal with custom object type attribute and no matching items",
			results: []map[string]types.AttributeValue{
				{
					"id":                 &types.AttributeValueMemberS{Value: "9"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "appliance"},
					"other_field":        &types.AttributeValueMemberS{Value: "oven"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
			},
			entityTypes:               []string{"vehicle"},
			customObjectTypeAttribute: "custom_entity_type",
			expectedData:              mockDataItems{},
			expectedErr:               "",
		},
		{
			name: "Unmarshal with missing custom object type attribute",
			results: []map[string]types.AttributeValue{
				{
					"id":          &types.AttributeValueMemberS{Value: "10"},
					"other_field": &types.AttributeValueMemberS{Value: "item without entity type"},
				},
			},
			entityTypes:               []string{"vehicle"},
			customObjectTypeAttribute: "custom_entity_type",
			expectedData:              mockDataItems{},
			expectedErr:               "",
		},
		{
			name: "Unmarshal with custom object type attribute and with separator",
			results: []map[string]types.AttributeValue{
				{
					"id":                 &types.AttributeValueMemberS{Value: "11"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "vehicle#UUID"},
					"other_field":        &types.AttributeValueMemberS{Value: "car"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
				{
					"id":                 &types.AttributeValueMemberS{Value: "12"},
					"custom_entity_type": &types.AttributeValueMemberS{Value: "vehicle#UUID"},
					"other_field":        &types.AttributeValueMemberS{Value: "bike"},
					"_object_type":       &types.AttributeValueMemberS{Value: "prefix#UUID"},
				},
			},
			entityTypes:               []string{"vehicle"},
			customObjectTypeAttribute: "custom_entity_type",
			expectedData: mockDataItems{
				{ID: "11", CustomEntityType: "vehicle#UUID", OtherField: "car", DefaultEntityType: "prefix#UUID"},
				{ID: "12", CustomEntityType: "vehicle#UUID", OtherField: "bike", DefaultEntityType: "prefix#UUID"},
			},
			expectedErr: "",
			separator:   "#",
		},
		{
			name: "Unmarshal with default object type attribute",
			results: []map[string]types.AttributeValue{
				{
					"id":           &types.AttributeValueMemberS{Value: "10"},
					"other_field":  &types.AttributeValueMemberS{Value: "item without entity type"},
					"_object_type": &types.AttributeValueMemberS{Value: "prefix"},
				},
				{
					"id":           &types.AttributeValueMemberS{Value: "11"},
					"other_field":  &types.AttributeValueMemberS{Value: "item without entity type"},
					"_object_type": &types.AttributeValueMemberS{Value: "otherPrefix"},
				},
			},
			entityTypes:               []string{"prefix"},
			customObjectTypeAttribute: "",
			expectedData: mockDataItems{
				{ID: "10", CustomEntityType: "", OtherField: "item without entity type", DefaultEntityType: "prefix"},
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prepare the output
			item := &Item{
				c: &Client{
					gsis: []gsi{
						{
							indexName:    "_object_type_index",
							partitionKey: "_object_type",
						},
					},
				},
				indexName:                 "_object_type_index",
				customObjectTypeAttribute: "",
				projection:                "",
				useGSI:                    false,
				item:                      nil,
				err:                       nil,
				batchData:                 keys{},
				pagination:                pagination{},
				filter:                    expression.ConditionBuilder{},
				condition:                 expression.ConditionBuilder{},
				key:                       map[string]types.AttributeValue{},
				keyCondition:              expression.KeyConditionBuilder{},
			}
			o := &output{
				Results: tt.results,
				item:    item,
				ctx:     context.Background(),
			}

			if tt.customObjectTypeAttribute != "" {
				o = o.WithCustomObjectTypeAttribute(tt.customObjectTypeAttribute)
			}

			if tt.bypassAuth {
				o = o.BypassAuthorization()
			}

			if tt.separator != "" {
				o.item.c.keySeparator = tt.separator
			}

			// Prepare the Out implementation
			var outData mockDataItems

			// Call Unmarshal
			o = o.Unmarshal(&outData, tt.entityTypes)

			// Run
			err := o.Run()

			if tt.expectedErr != "" {
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.Nil(t, err)
				// Verify the data
				assert.Equal(t, tt.expectedData, outData)
				if tt.customObjectTypeAttribute != "" {
					assert.Equal(t, tt.customObjectTypeAttribute, o.getObjectTypeAttribute())
				} else {
					assert.Equal(t, "_object_type", o.getObjectTypeAttribute())
				}
			}
		})
	}
}
