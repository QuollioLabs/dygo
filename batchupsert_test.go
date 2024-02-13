package dygo

import (
	"context"
	"log"
	"testing"
	"time"
)

func Test_batchupsert_item_happy_path(t *testing.T) {
	gId := newPK("room")

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	ids := make([]string, 0)
	newItem := new(Item)
	for i := 0; i < 5; i++ {
		d := dataItem{
			PK:           gId,
			SK:           "current",
			PhysicalName: "physical_name_1145",
			LogicalName:  "logical_name_1145",
			EntityType:   "room",
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		db.Item(d).AddBatchUpsertItem(newItem)
		ids = append(ids, gId)
		gId = newPK("room")
	}

	err = newItem.BatchUpsertItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	// validate item
	for _, id := range ids {
		fetchAndValidateItem(t, db, id, "current", true)
		removeItem(t, id, "current")
	}
}

func Test_batchupsert_item_invalid_data(t *testing.T) {
	gId := newPK("room")

	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	ids := make([]string, 0)
	newItem := new(Item)
	for i := 0; i < 5; i++ {
		d := dataItem{
			PK:           gId,
			SK:           "current",
			PhysicalName: "physical_name_1145",
			LogicalName:  "logical_name_1145",
			EntityType:   "room",
		}

		if i == 0 {
			d.EntityType = "table1"
		}
		db.Item(d).AddBatchUpsertItem(newItem)
		ids = append(ids, gId)
		gId = newPK("room")
	}

	err = newItem.BatchUpsertItem(context.Background(), 10)
	if err == nil {
		log.Fatal("error expected but no error")
	}

	// validate item
	for _, id := range ids {
		fetchAndValidateItem(t, db, id, "current", false)
	}
}

func Test_batchupsert_raw_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	newItem := new(Item)
	items, ids := getRawItem("room-", 500)
	for _, item := range items {
		db.ItemRaw(item).AddBatchUpsertRawItem(newItem)
	}

	items1, ids1 := getRawItem("room-", 300)
	for _, item := range items1 {
		db.ItemRaw(item).AddBatchUpsertRawItem(newItem)
	}

	err = newItem.BatchUpsertItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	ids = append(ids, ids1...)

	// validate item
	for _, id := range ids {
		fetchAndValidateItem(t, db, id, "current", true)
		removeItem(t, id, "current")
	}
}
