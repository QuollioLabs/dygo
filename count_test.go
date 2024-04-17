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
