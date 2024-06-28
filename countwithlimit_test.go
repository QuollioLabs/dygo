package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_countwithlimit_with_gsi_with_morethanlimit_items(t *testing.T) {
	limit := 2
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, 6000, prefix, blank, false)
	SK := "current"

	_, filterCount, err := db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		CountWithLimt(context.Background(), limit)

	if err != nil {
		log.Fatal(err)
	}

	// in one shot query operation can read 4732 items
	if filterCount != limit {
		t.Fatalf("expected %v items but got %v", limit, filterCount)
	}

	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}

func Test_countwithlimit_with_gsi_with_lessthanlimit_items(t *testing.T) {
	limit := 100
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	itemCount := 60
	prefix := "name_test_"
	gIds := createItemWithPrefix(t, true, itemCount, prefix, blank, false)
	SK := "current"

	_, filterCount, err := db.
		GSI("gsi-name", "room", Equal("current")).
		Filter("physical_name", KeyBeginsWith(prefix)).
		AndFilter("logical_name", KeyBeginsWith(prefix)).
		CountWithLimt(context.Background(), limit)

	if err != nil {
		log.Fatal(err)
	}

	// in one shot query operation can read 4732 items
	if filterCount != itemCount {
		t.Fatalf("expected %v items but got %v", itemCount, filterCount)
	}

	// remove item
	for _, v := range gIds {
		removeItem(t, v, SK)
	}
}
