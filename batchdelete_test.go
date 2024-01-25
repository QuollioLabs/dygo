package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_batchdelete_item_happy_path(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchDeleteItem(item)
	}

	err = item.BatchDeleteItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range gIds {
		fetchAndValidateItem(t, db, v, SK, false)
	}
}

func Test_batchdelete_item_without_tablename(t *testing.T) {
	db, err := getClient(blank, false)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, true, 2)

	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchDeleteItem(item)
	}

	err = item.BatchDeleteItem(context.Background(), 10)
	if err == nil {
		removeItems(t, gIds, SK)
		log.Fatal("error expected")
	}

	removeItems(t, gIds, SK)
}
