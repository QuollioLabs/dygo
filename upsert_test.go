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

func Test_update_item_with_condition_true(t *testing.T) {
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
		Version:      11,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version", ConditionEqual(0)).
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

func Test_update_item_with_condition_false(t *testing.T) {
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
		Version:      12,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version", ConditionEqual(10)).
		Upsert(context.Background())

	if err == nil {
		log.Fatalf("expected to get error, got nil")
	}

	// check if item exists
	data := get(t, db, PK, SK)
	if data.PK == "" {
		t.Fatalf("item not found")
	}

	if data.Version != 0 {
		t.Fatalf("expected physical_name to be updated")
	}

	// remove item
	removeItem(t, PK, SK)
}

func Test_insert_item_with_condition_false(t *testing.T) {
	db, err := getClient(blank, true)
	if err != nil {
		t.Fatalf("unexpected error : %v", err)
	}

	gIds := createItem(t, true, 1)
	SK := "current"
	PK := gIds[0]
	removeItem(t, PK, SK)

	newData := dataItem{
		PK:           PK,
		SK:           SK,
		EntityType:   "room",
		PhysicalName: "updated",
		LogicalName:  "updated",
		Version:      12,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version", ConditionEqual(10)).
		Upsert(context.Background())

	if err == nil {
		log.Fatalf("expected to get error, got nil")
	}

	// check if item exists
	data := get(t, db, PK, SK)
	if data.PK != "" {
		t.Fatalf("item not found expected, got %v", data.PK)
	}
}

func Test_update_item_with_condition_attributeExist_true(t *testing.T) {
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
		Version:      12,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version", ConditionAttributeExists()).
		Upsert(context.Background())
	if err != nil {
		log.Fatalf("unexpected error in updating item : %v", err)
	}

	// check if item exists
	data := get(t, db, PK, SK)
	if data.PK == "" {
		t.Fatalf("item not found")
	}

	// remove item
	removeItem(t, PK, SK)
}

func Test_update_item_with_condition_attributeExist_false(t *testing.T) {
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
		Version:      12,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version1", ConditionAttributeExists()).
		Upsert(context.Background())
	if err == nil {
		log.Fatalf("expected to get error, got nil")
	}

	// remove item
	removeItem(t, PK, SK)
}

func Test_update_item_with_condition_attributeNotExist_true(t *testing.T) {
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
		Version:      12,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err = db.
		Item(newData).
		Condition("version1", ConditionAttributeNotExists()).
		Upsert(context.Background())
	if err != nil {
		log.Fatalf("unexpected error in updating item : %v", err)
	}

	// check if item exists
	data := get(t, db, PK, SK)
	if data.PK == "" || data.Version != 12 {
		t.Fatalf("item not found")
	}

	// remove item
	removeItem(t, PK, SK)
}
