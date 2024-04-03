package dygo

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
)

// ConditionFunc returns a condition builder for an attribute.
type ConditionFunc func(string) expression.ConditionBuilder

// ConditionEqual returns a ConditionFunc that checks if the attribute value is equal to the specified value.
func ConditionEqual(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Equal(expression.Value(value))
	}
}

// ConditionBeginsWith returns a ConditionFunc that checks item based on whether the attribute's value begins with the specified prefix.
func ConditionBeginsWith(prefix any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).BeginsWith(prefix.(string))
	}
}

// ConditionBetween returns a ConditionFunc that checks if the attribute value is between two specified values.
func ConditionBetween(start, end any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).Between(expression.Value(start), expression.Value(end))
	}
}

// ConditionLessThan returns a ConditionFunc that checks if the attribute value is less than the specified value.
func ConditionLessThan(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).LessThan(expression.Value(value))
	}
}

// ConditionLessThanOrEqual returns a ConditionFunc that checks if the attribute value is less than or equal to the specified value.
func ConditionLessThanOrEqual(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).LessThanEqual(expression.Value(value))
	}
}

// ConditionGreaterThan returns a ConditionFunc that checks if the attribute value is greater than the specified value.
func ConditionGreaterThan(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).GreaterThan(expression.Value(value))
	}
}

// ConditionGreaterThanOrEqual returns a ConditionFunc that checks if the attribute value is greater than or equal to the specified value.
func ConditionGreaterThanOrEqual(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).GreaterThanEqual(expression.Value(value))
	}
}

// ConditionNotEqual returns a ConditionFunc that checks if the attribute value is not equal to the specified value.
func ConditionNotEqual(value any) ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).NotEqual(expression.Value(value))
	}
}

// ConditionIn returns a ConditionFunc that checks if the attribute value is one of the specified values.
func ConditionIn(value any) ConditionFunc {
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

// ConditionAttributeExists returns a ConditionFunc that checks if the specified attribute exists in the item.
func ConditionAttributeExists() ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).AttributeExists()
	}
}

// ConditionAttributeNotExists returns a ConditionFunc that checks if the specified attribute does not exist in the item.
func ConditionAttributeNotExists() ConditionFunc {
	return func(keyName string) expression.ConditionBuilder {
		return expression.Name(keyName).AttributeNotExists()
	}
}
