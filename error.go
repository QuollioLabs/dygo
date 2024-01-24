package dygo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

type dError struct {
	Function     string
	ErrorMessage error
}

const (
	errDygoError           = "DygoError"
	errMissingTableName    = "table name is missing"
	errMissingPartitionKey = "partition key is missing"
	errMissingRegion       = "region is missing"
	errMissingClient       = "something went wrong while creating the client"
)

// Error returns the error message associated with the Error struct.
// It formats the error message with the error type, method name, and error message.
func (e *dError) Error() string {
	return fmt.Sprintf("%s:: method : %s() message: %s", errDygoError, e.Function, e.ErrorMessage)
}

// DynamoError returns a new instance of Error.
func dynamoError() *dError {
	return &dError{}
}

// Method sets the function name associated with the error.
func (e *dError) method(method string) *dError {
	e.Function = method
	return e
}

// Message sets the error message for the Error instance.
func (e *dError) message(msg string) *dError {
	e.ErrorMessage = errors.New(msg)
	return e
}

// getDynamoDBError returns a custom error based on the type of error encountered in DynamoDB operations.
func getDynamoDBError(method string, err error) error {
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
			return dynamoError().method(method).message("key doesnt exist")
		}
		if method == "Create()" {
			return dynamoError().method(method).message("duplicate item")
		}
		return dynamoError().method(method).message("key doesnt exist")
	case errors.As(err, &dce):
		return dynamoError().method(method).message("duplicate item")
	case errors.As(err, &infe):
		return dynamoError().method(method).message("index doesnt exist")
	case errors.As(err, &ise):
		return dynamoError().method(method).message("internal server error")
	case errors.As(err, &rnfe):
		return dynamoError().method(method).message("table/index doesnt exist")
	case errors.As(err, &tnfe):
		return dynamoError().method(method).message("table doesnt exist")
	}
	return nil
}
