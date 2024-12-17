package dygo

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

func Test_scanauthorize_item(t *testing.T) {

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 5)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Project("_partition_key", "_entity_type", "_sort_key").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	if len(data) != 5 {
		t.Fatalf("expected 5 items but got %v", len(data))
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_scan_item_with_filter(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	targetPrefix := "name_test_1"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyBeginsWith(targetPrefix)).
		AndFilter("logical_name", KeyBeginsWith(targetPrefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 items but got %v", len(data))
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_scan_with_invalidandfilter(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		log.Fatal("error expected for invalid And filter")
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_invalidorfilter(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		OrFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		log.Fatal("error expected for invalid Or filter")
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_without_tablename(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		removeItems(t, gIds, SK)
		t.Fatal("expect error for table name, got nil")
	}

	removeItems(t, gIds, SK)
}

func Test_scan_with_keyseparator(t *testing.T) {
	separator := "#"
	db, err := getClient(separator, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, separator, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if d.EntityType != "room"+separator {
			removeItems(t, gIds, SK)
			t.Fatalf("_entity_type mismatch, expected table#1 got: %v", d.EntityType)
		}
		if exist := stringExists(gIds, d.PK); !exist {
			removeItems(t, gIds, SK)
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	if len(data) != 5 {
		removeItems(t, gIds, SK)
		t.Fatalf("expected 1 items but got %v", len(data))
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_filter_keycontains(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyContains("0")).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _global_id : %v not found", d.PK)
		}
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 items but got %v", len(data))
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_filter_keynotcontains(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyNotContains("0")).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _global_id : %v not found", d.PK)
		}
	}
	if len(data) != 4 {
		t.Fatalf("expected 4 items but got %v", len(data))
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_filter_keyin(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyIn("name_test_0, name_test_1")).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _global_id : %v not found", d.PK)
		}
	}
	if len(data) != 2 {
		t.Fatalf("expected 2 items but got %v", len(data))
	}
	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_RunAndFetchLastKey(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix1 := "name_test_1_"
	gIds := createItemWithPrefix(t, true, 5000, prefix1, blank, false)

	prefix2 := "name_test_2_"
	gIds = append(gIds, createItemWithPrefix(t, true, 150, prefix2, blank, false)...)

	SK := "current"
	count := 0

	InitialLEK := make(map[string]any)
	for {
		var data dataSlice
		FetchedLEK, err := db.
			InitScan().
			Filter("physical_name", KeyBeginsWith("name_test_2_")).
			AndFilter("logical_name", KeyBeginsWith(prefix2)).
			Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
			Limit(250).
			LastEvaluatedKey(InitialLEK).
			Scan(context.Background()).
			Unmarshal(&data, []string{"room"}).
			RunAndFetchLastKey()

		if err != nil {
			log.Fatal(err)
		}

		count += len(data)

		// Iterate through the FetchedLEK and convert the values
		for key, value := range FetchedLEK {
			var v any
			err := attributevalue.Unmarshal(value, &v)
			if err != nil {
				fmt.Printf("Failed to unmarshal value for key %s: %v\n", key, err)
				continue
			}
			InitialLEK[key] = v
		}

		if len(FetchedLEK) == 0 {
			break
		}
	}

	if count != 150 {
		t.Fatalf("expected 150 items but got %v", count)
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_scan_with_bypassAuth(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank, false)
	SK := "current"
	var data dataSlice

	err = db.
		InitScan().
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Scan(context.Background()).
		BypassAuthorization().
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	if len(data) != 5 {
		t.Fatalf("expected 1 items but got %v", len(data))
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}
