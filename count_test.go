package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_count_item(t *testing.T) {

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 5)
	SK := "current"
	PK := gIds[0]

	_, filterCount, err := db.
		PK(PK).
		SK(BeginsWith("current")).
		Count(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if filterCount != 1 {
		t.Fatalf("expected 1 items but got %v", filterCount)
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_count_with_gsi(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 51, prefix, blank, false)
	SK := "current"

	_, filterCount, err := db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Count(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if filterCount != 51 {
		t.Fatalf("expected 51 items but got %v", filterCount)
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_count_item_pkskUnique_(t *testing.T) {

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds, sks := createItemWithSK(t, true, 5, "current")
	PK := gIds[0]

	_, filterCount, err := db.
		PK(PK).
		SK(BeginsWith("current_0")).
		Count(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if filterCount != 1 {
		t.Fatalf("expected 1 items but got %v", filterCount)
	}
	// remove item
	for i, v := range gIds {
		removeItem(t, v, sks[i])
	}
}

func Test_count_with_gsi_and_limit(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	totalItem := 51
	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, totalItem, prefix, blank, false)
	SK := "current"
	limit := 5

	totalCount, filterCount, err := db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Limit(limit).
		Count(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if totalCount != totalItem {
		t.Fatalf("expected %v items but got %v", totalItem, totalCount)
	}
	if filterCount != limit {
		t.Fatalf("expected %v items but got %v", limit, filterCount)
	}
	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_count_with_gsi_and_item_morethan_limit(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	totalItem := 51
	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, totalItem, prefix, blank, false)
	SK := "current"
	limit := 500

	totalCount, filterCount, err := db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		Limit(limit).
		Count(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	if totalCount != totalItem {
		removeItems(t, gIds, SK)
		t.Fatalf("expected %v items but got %v", totalItem, totalCount)
	}
	if filterCount != totalItem {
		removeItems(t, gIds, SK)
		t.Fatalf("expected %v items but got %v", totalItem, filterCount)
	}
	// remove item
	removeItems(t, gIds, SK)
}
