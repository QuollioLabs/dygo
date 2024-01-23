package dygo

import (
	"context"
	"testing"
)

func Test_get_item_happy_path(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 1)
	SK := "current"
	PK := gIds[0]

	d := dataItem{}
	err = db.
		PK(PK).
		SK(Equal(SK)).
		GetItem(context.Background(), &d)
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	if d.PK != PK {
		t.Fatalf("expected _partition_key : %v but got %v", PK, d.PK)
	}

	if exist := stringExists(gIds, d.PK); !exist {
		t.Fatalf("expected _partition_key : %v not found", d.PK)
	}

	// remove item
	removeItem(t, db, PK, SK)
}

func Test_getauthorized_item_happy_path(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 1)
	SK := "current"
	PK := gIds[0]

	d := dataItem{}
	err = db.
		PK(PK).
		SK(Equal(SK)).
		GetAuthorizedItem(context.Background(), &d)
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	switch d.IsAuthorized {
	case false:
		if d.PK == "" || d.EntityType != "" {
			t.Fatalf("expected _partition_key : %v but got blank, expected _entity_type : blank but got %v", d.PK, d.EntityType)
		}
	case true:
		if d.PK == "" || d.EntityType == "" {
			t.Fatalf("expected _partition_key : %v but got blank, expected _entity_type : %v but got blank", d.PK, d.EntityType)
		}
	}

	// remove item
	removeItem(t, db, PK, SK)
}

func Test_getauthorized_item_not_found(t *testing.T) {
	db, err := getClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 1)
	SK := "current"
	PK := gIds[0]

	d := dataItem{}
	err = db.
		PK(PK+"+change_key").
		SK(Equal(SK)).
		GetAuthorizedItem(context.Background(), &d)
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	if d.PK != "" {
		t.Fatalf("expected _partition_key : blank but got %v", d.PK)
	}

	// remove item
	removeItem(t, db, PK, SK)
}
