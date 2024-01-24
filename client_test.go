package dygo

import (
	"testing"
)

func Test_client_happy_path(t *testing.T) {
	dbEndpoint := "http://localhost:8000"
	_, err := NewClient(
		WithTableName("test-table-1"),
		WithRegion("ap-northeast-1"),
		WithPartitionKey("_partition_key"),
		WithSortKey("_sort_key"),
		WithGSI("gsi-name", "_entity_type", "_sort_key"),
		WithEndpoint(dbEndpoint),
	)

	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}
}

func Test_client_without_region(t *testing.T) {
	dbEndpoint := "http://localhost:8000"
	_, err := NewClient(
		WithTableName("test-table-1"),
		WithPartitionKey("_partition_key"),
		WithSortKey("_sort_key"),
		WithGSI("gsi-name", "_entity_type", "_sort_key"),
		WithEndpoint(dbEndpoint),
	)

	expectedErr := dynamoError().method("NewClient").message(errMissingRegion).Error()

	if err == nil || err.Error() != expectedErr {
		t.Fatalf("expected error : %v got : %v", expectedErr, err)
	}
}

func Test_client_without_table(t *testing.T) {
	dbEndpoint := "http://localhost:8000"
	_, err := NewClient(
		WithRegion("ap-northeast-1"),
		WithPartitionKey("_partition_key"),
		WithSortKey("_sort_key"),
		WithGSI("gsi-name", "_entity_type", "_sort_key"),
		WithEndpoint(dbEndpoint),
	)

	if err != nil {
		t.Fatalf("expected no error got : %v", err)
	}
}
