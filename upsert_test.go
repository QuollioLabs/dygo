package dygo

import (
	"context"
	"log"
	"testing"
	"time"
)

func Test_upsert_item(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 1)
	SK := "current"
	PK := gIds[0]

	newData := dataItem{
		PK:           PK,
		SK:           SK,
		EntityType:   "room",
		PhysicalName: "updated",
		LogicalName:  "updated",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Upsert(context.Background())

	if err != nil {
		log.Fatalf("unexpected error in creating item : %v", err)
	}

	// check if item exists
	data := get(t, db, PK, SK)
	if data.PK == "" {
		t.Fatalf("item not found")
	}

	if data.PhysicalName != "updated" {
		t.Fatalf("expected physical_name to be updated")
	}

	// remove item
	removeItem(t, PK, SK)

}
