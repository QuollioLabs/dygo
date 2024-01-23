package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_batchdelete_item_happy_path(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, db, 2)

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
