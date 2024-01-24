package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_upsert_item(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 1)
	SK := "current"
	PK := gIds[0]

	newData := dataItem{
		PK:           PK,
		SK:           SK,
		EntityType:   "room",
		PhysicalName: "updated",
		LogicalName:  "updated",
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
	removeItem(t, db, PK, SK)

}
