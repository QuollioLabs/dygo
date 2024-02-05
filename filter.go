package dygo

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// FilterFunc returns a condition builder for a key.
type FilterFunc func(string) expression.ConditionBuilder

// KeyEqual returns a FilterFunc that filters based on the equality of a key's value.
func KeyEqual(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Equal(expression.Value(value))
	}
}

// KeyBeginsWith returns a FilterFunc that filters items based on whether the key begins with the specified prefix.
func KeyBeginsWith(prefix any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).BeginsWith(prefix.(string))
	}
}

// KeyBetween returns a FilterFunc that generates a condition builder for a keyName
// that checks if its value is between the start and end values.
// The start and end values can be of any type.
func KeyBetween(start, end any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Between(expression.Value(start), expression.Value(end))
	}
}

// KeyLessThan returns a FilterFunc that filters items based on whether the value of the specified key is less than the given value.
func KeyLessThan(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).LessThan(expression.Value(value))
	}
}

// KeyLessThanOrEqual returns a FilterFunc that filters items where the value of the specified key is less than or equal to the given value.
func KeyLessThanOrEqual(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).LessThanEqual(expression.Value(value))
	}
}

// KeyGreaterThan returns a FilterFunc that filters items based on the specified key being greater than the given value.
func KeyGreaterThan(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).GreaterThan(expression.Value(value))
	}
}

// KeyGreaterThanOrEqual returns a FilterFunc that filters items where the value of the specified key is greater than or equal to the given value.
func KeyGreaterThanOrEqual(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).GreaterThanEqual(expression.Value(value))
	}
}

// KeyNotEqual returns a FilterFunc that generates a condition builder for a key
// that is not equal to the specified value.
func KeyNotEqual(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).NotEqual(expression.Value(value))
	}
}

// KeyContains returns a FilterFunc that checks if the given value is contained in the key's value.
func KeyContains(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Contains(getStringValue(value))
	}
}

// KeyNotContains returns a FilterFunc that filters out items where the value of the specified key does not contain the given value.
func KeyNotContains(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Contains(getStringValue(value)).Not()
	}
}

// KeyNotNull returns a FilterFunc that checks if the specified key is not null.
func KeyNotNull() FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).AttributeExists()
	}
}

// KeyNull returns a FilterFunc that checks if the specified key does not exist in the DynamoDB item.
func KeyNull() FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).AttributeNotExists()
	}
}

// KeyIn returns a FilterFunc that checks if the given value is present in the specified keyName.
func KeyIn(value any) FilterFunc {
	return func(keyName string) expression.ConditionBuilder {
		csv := getStringValue(value)
		if csv != "" {
			// Split the comma-separated string into a slice of values.
			values := strings.Split(csv, ",")
			// Create a slice of OperandBuilders from the values.
			operands := make([]expression.OperandBuilder, len(values))
			for i, v := range values {
				operands[i] = expression.Value(strings.TrimSpace(v))
			}
			// Return the ConditionBuilder with the IN condition for the keyName.
			return expression.Name(keyName).In(operands[0], operands[1:]...)
		}
		return expression.Name(keyName).In(expression.Value(csv))
	}
}
