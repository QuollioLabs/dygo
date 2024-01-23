package dygo

import (
	"context"
	"log"
	"testing"
)

func Test_batchgetauthorized_item_happy_path(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, db, 4)
	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err != nil {
		log.Fatal(err)
	}

	if len(data) != len(gIds) {
		t.Fatalf("expected %v items but got %v", len(gIds), len(data))
	}

	for _, d := range data {
		if exist := stringExists(gIds, d.PK); !exist {
			t.Fatalf("expected _partition_key : %v not found", d.PK)
		}
	}
	// remove item
	for _, gId := range gIds {
		removeItem(t, db, gId, SK)
	}
}

func Test_batchgetauthorized_item_omitEmptyKeys_false(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, db, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, false)
	}

	var data dataSlice
	err = item.BatchGetAuthorizedItem(context.Background(), 10).
		Unmarshal(&data, []string{"room"}).
		Run()
	if err == nil {
		for _, gId := range gIds {
			removeItem(t, db, gId, SK)
		}
		log.Fatal(err)
	}
	for _, v := range gIds {
		removeItem(t, db, v, SK)
	}
}

func Test_batchget_item_happy_path(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, db, 4)

	item := new(Item)
	for _, gId := range gIds {
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, true)
	}

	output, err := item.BatchGetItem(context.Background(), 10)
	if err != nil {
		log.Fatal(err)
	}

	if output != nil {
		if len(output) != len(gIds) {
			t.Fatalf("expected %v items but got %v", len(gIds), len(output))
		}
	}
	// remove item
	for _, gId := range gIds {
		removeItem(t, db, gId, SK)
	}
}

func Test_batchget_item_omitEmptyKeys_false(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	SK := "current"
	gIds := createItem(t, db, 2)

	item := new(Item)
	for i, gId := range gIds {
		if i%2 == 0 {
			gId = ""
		}
		db.PK(gId).SK(Equal(SK)).AddBatchGetItem(item, false)
	}

	_, err = item.BatchGetItem(context.Background(), 10)
	if err == nil {
		for _, gId := range gIds {
			removeItem(t, db, gId, SK)
		}
		log.Fatal(err)
	}
	for _, v := range gIds {
		removeItem(t, db, v, SK)
	}
}
