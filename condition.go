package dygo

import "github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

// SortKeyFunc returns a KeyConditionBuilder and a value of any type.
type SortKeyFunc func(string) (expression.KeyConditionBuilder, any)

// Equal returns a SortKeyFunc that generates a KeyConditionBuilder for the "Equal" condition.
// The generated KeyConditionBuilder checks if the value of the specified key is equal to the given value.
// It takes a value of any type as a parameter and returns a function that accepts a key name.
func Equal(value any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).Equal(expression.Value(value)), value
	}
}

// BeginsWith returns a SortKeyFunc that can be used to create a KeyConditionBuilder
// for filtering items based on a prefix match with the given prefix value.
// The prefix value should be of type string.
func BeginsWith(prefix any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).BeginsWith(prefix.(string)), prefix
	}
}

// Between is a function that returns a SortKeyFunc which represents a key condition builder for the BETWEEN condition in DynamoDB.
// It takes two parameters, start and end, of any type.
// The SortKeyFunc returned by Between can be used to create a key condition builder for a specific key name.
// The start value is inclusive and the end value is exclusive in the BETWEEN condition.
func Between(start, end any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).Between(expression.Value(start), expression.Value(end)), start
	}
}

// LessThan returns a SortKeyFunc that generates a KeyConditionBuilder for a less than condition.
// The generated KeyConditionBuilder compares the specified keyName with the given value using the LessThan operator.
// The value parameter represents the value to compare against.
func LessThan(value any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).LessThan(expression.Value(value)), value
	}
}

// LessThanOrEqual returns a SortKeyFunc that generates a KeyConditionBuilder for a less than or equal to comparison.
// The generated KeyConditionBuilder compares the given keyName with the provided value using the LessThanEqual method of the expression package.
func LessThanOrEqual(value any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).LessThanEqual(expression.Value(value)), value
	}
}

// GreaterThan returns a SortKeyFunc that generates a KeyConditionBuilder for a greater than condition.
// The SortKeyFunc takes a value of any type and returns a function that accepts a keyName string.
func GreaterThan(value any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).GreaterThan(expression.Value(value)), value
	}
}

// GreaterThanOrEqual returns a SortKeyFunc that generates a KeyConditionBuilder for a greater than or equal condition.
// The generated KeyConditionBuilder compares the specified keyName with the given value and returns items where the key value is greater than or equal to the specified value.
func GreaterThanOrEqual(value any) SortKeyFunc {
	return func(keyName string) (expression.KeyConditionBuilder, any) {
		return expression.Key(keyName).GreaterThanEqual(expression.Value(value)), value
	}
}
