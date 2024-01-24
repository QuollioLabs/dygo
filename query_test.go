package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_queryauthorize_item(t *testing.T) {

	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 5)
	SK := "current"
	PK := gIds[0]
	var data dataSlice

	err = db.
		PK(PK).
		SK(BeginsWith("current")).
		Project("_partition_key", "_entity_type", "_sort_key").
		QueryAuthorizeItem(context.Background()).
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
		removeItem(t, db, v, SK)
	}
}

func Test_query_item(t *testing.T) {

	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 5)
	SK := "current"
	PK := gIds[0]
	var items []dataItem

	err = db.
		PK(PK).
		SK(BeginsWith("current")).
		Project("_partition_key", "_entity_type", "_sort_key").
		Query(context.Background(), &items)

	if err != nil {
		log.Fatal(err)
	}

	for _, d := range items {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 items but got %v", len(items))
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, db, v, SK)
	}
}

func Test_queryauthorize_with_gsi(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, db, 5, prefix)
	SK := "current"
	var data dataSlice

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		// Limit(2).
		// LastEvaluatedKey(lek).
		QueryAuthorizeItem(context.Background()).
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
		removeItem(t, db, v, SK)
	}
}

func Test_query_with_gsi(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, db, 5, prefix)
	SK := "current"
	var data []dataItem

	err = db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Project("_partition_key", "_entity_type", "_sort_key", "physical_name", "logical_name").
		// Limit(2).
		// LastEvaluatedKey(lek).
		Query(context.Background(), &data)

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
		removeItem(t, db, v, SK)
	}
}
