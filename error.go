package dygo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type Error struct {
	Function     string
	ErrorMessage error
}

const (
	ErrDygoError           = "DygoError"
	ErrMissingTableName    = "table name is missing"
	ErrMissingPartitionKey = "partition key is missing"
	ErrMissingRegion       = "region is missing"
	ErrMissingClient       = "something went wrong while creating the client"
)

// Error returns the error message associated with the Error struct.
// It formats the error message with the error type, method name, and error message.
func (e *Error) Error() string {
	return fmt.Sprintf("%s:: method : %s() message: %s", ErrDygoError, e.Function, e.ErrorMessage)
}

// DynamoError returns a new instance of Error.
func DynamoError() *Error {
	return &Error{}
}

// Method sets the function name associated with the error.
func (e *Error) Method(method string) *Error {
	e.Function = method
	return e
}

// Message sets the error message for the Error instance.
func (e *Error) Message(msg string) *Error {
	e.ErrorMessage = errors.New(msg)
	return e
}

// GetDynamoDBError returns a custom error based on the type of error encountered in DynamoDB operations.
func GetDynamoDBError(method string, err error) error {
	method = fmt.Sprintf("%s()", method)
	var cce *types.ConditionalCheckFailedException
	var dce *types.DuplicateItemException
	var infe *types.IndexNotFoundException
	var ise *types.InternalServerError
	var rnfe *types.ResourceNotFoundException
	var tnfe *types.TableNotFoundException

	switch {
	case errors.As(err, &cce):
		if method == "Delete()" {
			return DynamoError().Method(method).Message("key doesnt exist")
		}
		if method == "Create()" {
			return DynamoError().Method(method).Message("duplicate item")
		}
		return DynamoError().Method(method).Message("key doesnt exist")
	case errors.As(err, &dce):
		return DynamoError().Method(method).Message("duplicate item")
	case errors.As(err, &infe):
		return DynamoError().Method(method).Message("index doesnt exist")
	case errors.As(err, &ise):
		return DynamoError().Method(method).Message("internal server error")
	case errors.As(err, &rnfe):
		return DynamoError().Method(method).Message("table/index doesnt exist")
	case errors.As(err, &tnfe):
		return DynamoError().Method(method).Message("table doesnt exist")
	}
	return nil
}
