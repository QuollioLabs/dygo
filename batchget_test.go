package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_batchgetauthorized_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
		if d.LogicalName == "" {
			t.Fatalf("expected non-empty logical_name but got empty value")
		}
		if d.PhysicalName == "" {
			t.Fatalf("expected non-empty physical_name but got empty value")
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchgetauthorized_item_happy_path_with_projection(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).Project("_partition_key", "_entity_type", "_sort_key", "logical_name").AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.
		BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
		if d.LogicalName == "" {
			t.Fatalf("expected non-empty logical_name but got empty value")
		}
		if d.PhysicalName != "" {
			t.Fatalf("expected empty physical_name but got %v", d.PhysicalName)
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchgetauthorized_item_duplicate_keys(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchgetauthorized_item_omitEmptyKeys_false(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, false)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err == nil {
		removeItems(t, gIds, SK)
		log.Fatal(err)
	}
	removeItems(t, gIds, SK)
}

func Test_batchget_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)

	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	output, err := item.BatchGetItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	if output != nil {
		if len(output) != len(gIds) {
			t.Fatalf("expected %v items but got %v", len(gIds), len(output))
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchget_item_omitEmptyKeys_false(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, false)
	}

	_, err = item.BatchGetItem(context.Background(), 10)
	if err == nil {
		removeItems(t, gIds, SK)
		log.Fatal(err)
	}
	removeItems(t, gIds, SK)
}

func Test_batchget_item_without_tablename_happy_path(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)

	item := new(Item)
	for _, gId := range gIds {
		db.Table("test-table-1").PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	output, err := item.BatchGetItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	if output != nil {
		if len(output) != len(gIds) {
			t.Fatalf("expected %v items but got %v", len(gIds), len(output))
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchget_item_without_tablename(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)

	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	_, err = item.BatchGetItem(context.Background(), 10)
	if err == nil {
		log.Fatal("error expected")
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchget_item_omitEmptyKeys_true(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	out, err := item.BatchGetItem(context.Background(), 10)
	if err != nil {
		removeItems(t, gIds, SK)
		log.Fatalf("unexpected error : %v", err)
	}

	if out != nil {
		if len(out) != 1 {
			removeItems(t, gIds, SK)
			t.Fatalf("expected 1 item but got %v", len(out))
		}
	}

	removeItems(t, gIds, SK)
}

func Test_batchget_item_omitEmptyKeys_true_without_tablename(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	_, err = item.BatchGetItem(context.Background(), 10)
	if err == nil {
		removeItems(t, gIds, SK)
		log.Fatal("expected error")
	}

	removeItems(t, gIds, SK)
}

func Test_batchgetauthorized_item_withpages_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	// create 300 items
	gIds := createItem(t, true, 300)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	// BatchGetAuthorizedItem should return 300 items
	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_batchgetauthorized_item_bypassAuth_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 4)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		BypassAuthorization().
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	// remove item
	removeItems(t, gIds, SK)
}
