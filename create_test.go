package dygo

import (
	"context"
	"log"
	"testing"
	"time"
)

func Test_create_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	PK := newPK("room")
	SK := "current"

	newData := dataItem{
		PK:           PK,
		SK:           SK,
		EntityType:   "room",
		PhysicalName: "physical_name_11",
		LogicalName:  "logical_name_11",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Create(context.Background())

	if err != nil {
		log.Fatalf("unexpected error in creating item : %v", err)
	}

	// check if item exists
	fetchAndValidateItem(t, db, PK, SK, true)

	// remove item
	removeItem(t, PK, SK)
}

func Test_create_item_invalid_data(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	PK := newPK("room")
	SK := "current"

	newData := dataItem{
		PK:           PK,
		SK:           SK,
		EntityType:   "table1", // invalid entity type
		PhysicalName: "physical_name_11",
		LogicalName:  "logical_name_11",
	}

	err = db.
		Item(newData).
		Create(context.Background())

	if err == nil {
		log.Fatal("expected error in creating item but no error")
	}

	// check if item exists
	fetchAndValidateItem(t, db, PK, SK, false)
}
