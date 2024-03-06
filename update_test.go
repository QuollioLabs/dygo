package dygo

import (
	"context"
	"testing"
)

func Test_update_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 50)
	SK := "current"

	newData := getUpdateItem(gIds, 50)

	newItem := new(Item)
	for i := range newData {
		db.UpdateItemRaw(newData[i]).AddUpdateRawItem(newItem)
	}

	err = newItem.Update(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error in updating item : %v", err)
	}

	// validate item
	for _, id := range gIds {
		data := get(t, db, id, SK)
		if data.PhysicalName != "updated-"+id {
			t.Fatalf("expected physical_name to be updated-" + id)
		}
		removeItem(t, id, "current")
	}
}

func Test_update_item_not_exist(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := []string{newPK("room"), newPK("room"), newPK("room")}
	SK := "current"

	newData := getUpdateItem(gIds, 3)

	newItem := new(Item)
	for i := range newData {
		db.UpdateItemRaw(newData[i]).AddUpdateRawItem(newItem)
	}

	err = newItem.Update(context.Background(), 3)
	if err != nil {
		t.Fatalf("unexpected error in updating item : %v", err)
	}

	// validate item
	for _, id := range gIds {
		data := get(t, db, id, SK)
		if data.PhysicalName != "updated-"+id || data.LogicalName != "" {
			t.Fatalf("expected physical_name to be updated-" + id)
		}
		removeItem(t, id, "current")
	}
}
