package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_batchupsert_item_happy_path(t *testing.T) {
	gId := newPK("room")

	db, err := GetClient()
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
		removeItem(t, db, id, "current")
	}
}

func Test_batchupsert_item_invalid_data(t *testing.T) {
	gId := newPK("room")

	db, err := GetClient()
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
