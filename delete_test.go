package dygo

import (
	"context"
	"testing"
)

func Test_delete_item(t *testing.T) {
	db, err := GetClient()
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, db, 1)
	SK := "current"
	PK := gIds[0]

	err = db.
		PK(PK).
		SK(Equal(SK)).
		Delete(context.Background())
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	fetchAndValidateItem(t, db, PK, SK, false)
}
