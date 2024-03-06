package dygo

import (
	"errors"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// getItemExpression returns the expression used to retrieve an item from the database.
func (i *Item) getItemExpression() (*expression.Expression, error) {
	if err := i.isGetItemValid(); err != nil {
		return nil, err
	}
	builder := expression.NewBuilder()
	var expr expression.Expression
	if i.projection != "" {
		proj, err := projection(i.projection)
		if err != nil {
			return nil, err
		}
		builder = builder.WithProjection(*proj)

		expr, err = builder.Build()
		if err != nil {
			log.Fatalf("failed to build expression, %v", err)
		}
	}
	return &expr, nil
}

// getQueryExpression returns the query expression for the Item.
// It constructs and builds the expression using the provided projection, key condition, and filter (if set).
func (i *Item) getQueryExpression() (*expression.Expression, error) {
	keyCondition, err := getKeyCondition(i)
	if err != nil {
		return nil, err
	}
	builder := expression.NewBuilder().WithKeyCondition(*keyCondition)

	if i.projection != "" {
		proj, err := projection(i.projection)
		if err != nil {
			return nil, err
		}
		builder = builder.WithProjection(*proj)
	}

	if i.filter.IsSet() {
		builder = builder.WithFilter(i.filter)
	}
	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return &expr, nil
}

// getKeyCondition returns the KeyConditionBuilder associated with the given Item.
func getKeyCondition(i *Item) (*expression.KeyConditionBuilder, error) {
	return &i.keyCondition, nil
}

// projection takes a string representing a comma-separated list of field names and returns a ProjectionBuilder
// that can be used to build a projection expression for DynamoDB queries or scans.
func projection(p string) (*expression.ProjectionBuilder, error) {
	if p == "" {
		return nil, errors.New("projection string is empty")
	}
	projection := strings.Split(p, ",")
	if len(projection) == 0 {
		return nil, errors.New("no projection fields found")
	}
	proj := expression.NamesList(expression.Name(projection[0]))
	for _, fieldName := range projection[1:] {
		proj = proj.AddNames(expression.Name(fieldName))
	}
	return &proj, nil
}

// deleteItemExpression generates the expression for deleting an item in DynamoDB.
func (i *Item) deleteItemExpression() (expression.Expression, error) {
	condition := expression.AttributeExists(expression.Name(i.c.partitionKey))
	if i.c.sortKey != "" {
		condition = condition.And(expression.AttributeExists(expression.Name(i.c.sortKey)))
	}
	expr, err := expression.NewBuilder().WithCondition(condition).Build()
	if err != nil {
		return expression.Expression{}, err
	}
	return expr, nil
}

// createItemExpression creates an expression for conditional item creation.
func (i *Item) createItemExpression() (expression.Expression, error) {
	condition := expression.AttributeNotExists(expression.Name(i.c.partitionKey))
	if i.c.sortKey != "" {
		condition = condition.And(expression.AttributeNotExists(expression.Name(i.c.sortKey)))
	}
	expr, err := expression.NewBuilder().WithCondition(condition).Build()
	if err != nil {
		return expression.Expression{}, err
	}
	return expr, nil
}

// getUpdateItemExpression returns the expression for updating an item in DynamoDB.
func (i *Item) getUpdateItemExpression(index int) (*expression.Expression, error) {
	if err := i.isUpdateItemValid(index); err != nil {
		return nil, err
	}

	var updateBuilder expression.UpdateBuilder
	for attrName, attrValue := range i.batchData.updateItems[index].updateItem {
		updateBuilder = updateBuilder.Set(expression.Name(attrName), expression.Value(attrValue))
	}

	expr, err := expression.NewBuilder().WithUpdate(updateBuilder).Build()
	if err != nil {
		log.Fatalf("failed to build expression, %v", err)
	}
	return &expr, nil
}
