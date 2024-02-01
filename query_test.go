package dygo

import (
	"context"
	"fmt"
	"log"
	"testing"
)

func Test_queryauthorize_item(t *testing.T) {

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 5)
	SK := "current"
	PK := gIds[0]
	var data dataSlice

	err = db.
		PK(PK).
		SK(BeginsWith("current")).
		Project("_partition_key", "_entity_type", "_sort_key").
		Query(context.Background()).
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

func Test_queryauthorize_with_gsi(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		// Limit(2).
		// LastEvaluatedKey(lek).
		Query(context.Background()).
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

func Test_query_with_gsi_invalidandfilter(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Query(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		log.Fatal("error expected for invalid And filter")
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_query_with_gsi_invalidorfilter(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		OrFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Query(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		log.Fatal("error expected for invalid Or filter")
	}

	// remove item
	removeItems(t, gIds, SK)
}

func Test_query_with_gsi_without_tablename(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, blank)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		Query(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err == nil {
		removeItems(t, gIds, SK)
		t.Fatal("expect error for table name, got nil")
	}

	removeItems(t, gIds, SK)
}

func Test_query_with_keyseparator(t *testing.T) {
	separator := "#"
	db, err := getClient(separator, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 5, prefix, separator)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room"+separator, Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		Query(context.Background()).
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

func Test_query_with_filter_scanindexforward(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithVaringSortKey(t, true, 5, prefix, blank)
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", BeginsWith("current")).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		ScanIndexForward(true).
		Query(context.Background()).
		Unmarshal(&data, []string{"room"}).
		Run()

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(data); i++ {
		sk := fmt.Sprintf("current_%d", len(data)-1-i)
		if data[i].SK != sk {
			t.Fatalf("expected _sort_key : %v not found", sk)
		}
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
	for _, d := range data {
		removeItem(t, d.PK, d.SK)
	}
}
