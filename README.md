# dygo - DynamoDB Client for Go

Dygo is a user-friendly, feature-rich GoLang client for interfacing with AWS [DynamoDB](https://aws.amazon.com/dynamodb/). It offers a fluent interface with method chaining, optional authorization and data validation, simplifying the process of interacting with DynamoDB.

## Features

- **Easy-to-Use Interface**: Simple, intuitive methods for common DynamoDB operations.
- **Method Chaining**: Fluent interface allowing for cleaner, more readable code.
- **Authorization Support**: Optional authorization features to secure your database operations.
- **Data Validation**: Built-in validation to ensure data integrity.
- **Performance Optimized**: Minimized overhead for high efficiency in production environments.

## Installation

Install Dygo with `go get`:

```bash
go get github.com/QuollioLabs/dygo
```

## Quick Start
Here's a quick example to get you started:

```golang
package main

import (
    "github.com/QuollioLabs/dygo"
    "context"
)

func main() {
    // Initialize client
    db, err := dygo.NewClient(
        dygo.WithTableName("test-table"),
        dygo.WithRegion("ap-northeast-1"),
        dygo.WithPartitionKey("pk"),
        dygo.WithSortKey("sk"),
	)
    if err != nil {
        // handle error
    }

    // Example: Insert item
    err := db.
        Item(newData).
        Create(context.Background())
    if err != nil {
        // handle error
    }

    // Example: Fetching an item
    data := dataItem{}
    err = db.
        PK(PK).
        SK(dygo.Equal(SK)).
        GetItem(context.Background(), &data)
    if err != nil {
        // handle error
    }
}
```
