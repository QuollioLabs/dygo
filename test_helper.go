package dygo

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	ozzo "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/google/uuid"
)

type dataSlice []dataItem
type dataItem struct {
	PK           string `json:"_partition_key" dynamodbav:"_partition_key"`
	SK           string `json:"_sort_key" dynamodbav:"_sort_key"`
	PhysicalName string `json:"physical_name" dynamodbav:"physical_name"`
	LogicalName  string `json:"logical_name" dynamodbav:"logical_name"`
	EntityType   string `json:"_entity_type" dynamodbav:"_entity_type"`
	IsAuthorized bool   `json:"is_authorized" dynamodbav:"is_authorized"`
}

func (d dataItem) Validate() error {
	return ozzo.ValidateStruct(&d,
		ozzo.Field(&d.PK, ozzo.By(d.isValidPK())),
		ozzo.Field(&d.EntityType, ozzo.By(d.isValidEntityType())),
		ozzo.Field(&d.SK, ozzo.By(d.isValidSK())),
	)
}

func (d *dataItem) Authorize(ctx context.Context) error {
	d.IsAuthorized = isAuthorized()
	gId := d.PK
	if !d.IsAuthorized {
		*d = dataItem{}
		d.PK = gId
		d.IsAuthorized = false
		return nil
	}
	d.IsAuthorized = true
	return nil
}

func (d *dataSlice) Authorize(ctx context.Context) error {
	// write your own logic
	return nil
}

func lookupEntityType(pk string) string {
	switch {
	case pk[:2] == "rm":
		return "room"
	case pk[:3] == "inv":
		return "inventory"
	case pk[:3] == "htl":
		return "hotel"
	}
	return ""
}

func getEntityTypePrefix(entityType string) string {
	switch entityType {
	case "room":
		return "rm"
	case "inventory":
		return "inv"
	case "hotel":
		return "htl"
	}
	return ""
}

func (d *dataItem) isValidPK() ozzo.RuleFunc {
	return func(value interface{}) error {
		switch lookupEntityType(d.PK) {
		case "hotel":
		case "room":
		case "inventory":
		default:
			return fmt.Errorf("invalid pk '%s' for the model", d.PK)
		}
		return nil
	}
}

func (d *dataItem) isValidEntityType() ozzo.RuleFunc {
	return func(value interface{}) error {
		switch getEntityTypePrefix(d.EntityType) {
		case "htl":
		case "rm":
		case "inv":
		default:
			return fmt.Errorf("invalid entity type '%s' for the model", d.EntityType)
		}
		return nil
	}
}

func (d *dataItem) isValidSK() ozzo.RuleFunc {
	return func(value interface{}) error {
		switch {
		case d.SK == "current":
		default:
			return fmt.Errorf("invalid SK '%s' for the model", d.SK)
		}
		return nil
	}
}

func GetClient() (*Client, error) {
	dbEndpoint := "http://localhost:8000"
	db, err := NewClient(
		WithTableName("test-table-1"),
		WithRegion("ap-northeast-1"),
		WithPartitionKey("_partition_key"),
		WithSortKey("_sort_key"),
		WithGSI("gsi-name", "_entity_type", "_sort_key"),
		WithEndpoint(dbEndpoint),
	)

	return db, err
}

// function to generate random uuid
func newPK(prefix string) string {
	newUUID, err := uuid.NewUUID()
	if err != nil {
		fmt.Printf("Failed to generate UUID: %v", err)
		return ""
	}
	switch prefix {
	case "room":
		return fmt.Sprintf("%s-%s", "rm", newUUID.String())
	case "inventory":
		return fmt.Sprintf("%s-%s", "inv", newUUID.String())
	case "hotel":
		return fmt.Sprintf("%s-%s", "htl", newUUID.String())
	}
	return ""
}

func fetchAndValidateItem(t *testing.T, db *Client, PK string, SK string, found bool) {
	data := dataItem{}
	err := db.
		PK(PK).
		SK(Equal(SK)).
		GetItem(context.Background(), &data)
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	if found && data.PK != PK {
		t.Fatalf("expected _partition_key : %v not found", PK)
	}

	if !found && data.PK != "" {
		t.Fatalf("expected _partition_key : %v shouldn't be found", PK)
	}
}

func get(t *testing.T, db *Client, PK string, SK string) dataItem {
	data := dataItem{}
	err := db.
		PK(PK).
		SK(Equal(SK)).
		GetItem(context.Background(), &data)
	if err != nil {
		t.Fatalf("unexpected error in fetching item: %v", err)
	}

	return data
}

func removeItem(t *testing.T, db *Client, PK, SK string) {
	err := db.
		PK(PK).
		SK(Equal(SK)).Delete(context.Background())
	if err != nil {
		t.Logf("unexpected error in deleting item: %v", err)
	}
}

func createItem(t *testing.T, db *Client, count int) []string {
	gIds := make([]string, 0)
	SK := "current"

	for i := 0; i < count; i++ {
		PK := newPK("room")
		gIds = append(gIds, PK)

		newData := dataItem{
			PK:           PK,
			SK:           SK,
			EntityType:   "room",
			PhysicalName: fmt.Sprintf("physical_name_%d", i),
			LogicalName:  fmt.Sprintf("logical_name_%d", i),
		}

		err := db.
			Item(newData).
			Create(context.Background())

		if err != nil {
			t.Fatalf("unexpected error in creating item : %v", err)
		}
	}
	return gIds
}

func createItemWithPrefix(t *testing.T, db *Client, count int, prefix string) []string {
	gIds := make([]string, 0)
	SK := "current"

	for i := 0; i < count; i++ {
		PK := newPK("room")
		gIds = append(gIds, PK)

		newData := dataItem{
			PK:           PK,
			SK:           SK,
			EntityType:   "room",
			PhysicalName: fmt.Sprintf("%s%d", prefix, i),
			LogicalName:  fmt.Sprintf("%s%d", prefix, i),
		}

		err := db.
			Item(newData).
			Create(context.Background())

		if err != nil {
			t.Fatalf("unexpected error in creating item : %v", err)
		}
	}
	return gIds
}

func isAuthorized() bool {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	// Use the local generator to get a random integer, 0 or 1
	return r.Intn(2) == 1
}
