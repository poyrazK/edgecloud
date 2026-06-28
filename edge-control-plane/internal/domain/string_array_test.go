package domain

import (
	"encoding/json"
	"testing"

	"github.com/lib/pq"
)

func TestStringArrayFrom_Nil(t *testing.T) {
	result := StringArrayFrom(nil)
	if result == nil {
		t.Fatal("expected non-nil pq.StringArray from nil input")
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestStringArrayFrom_EmptySlice(t *testing.T) {
	result := StringArrayFrom([]string{})
	if result == nil {
		t.Fatal("expected non-nil pq.StringArray from empty slice")
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestStringArrayFrom_NonEmpty(t *testing.T) {
	input := []string{"a", "b", "c"}
	result := StringArrayFrom(input)
	if len(result) != 3 {
		t.Errorf("len = %d, want 3", len(result))
	}
	if result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("values = %v", result)
	}
}

func TestStringArrayTo_Nil(t *testing.T) {
	result := StringArrayTo(nil)
	if result != nil {
		t.Errorf("expected nil from nil input, got %v", result)
	}
}

func TestStringArrayTo_EmptyArray(t *testing.T) {
	input := pq.StringArray{}
	result := StringArrayTo(input)
	if result == nil {
		t.Fatal("expected non-nil []string from empty pq.StringArray")
	}
	if len(result) != 0 {
		t.Errorf("len = %d, want 0", len(result))
	}
}

func TestStringArrayTo_NonEmpty(t *testing.T) {
	input := pq.StringArray{"x", "y"}
	result := StringArrayTo(input)
	if len(result) != 2 {
		t.Errorf("len = %d, want 2", len(result))
	}
	if result[0] != "x" || result[1] != "y" {
		t.Errorf("values = %v", result)
	}
}

func TestStringArray_RoundTripThroughJSON(t *testing.T) {
	// pq.StringArray should marshal as a JSON array
	sa := pq.StringArray{"fra", "sfo"}
	data, err := json.Marshal(sa)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != `["fra","sfo"]` {
		t.Errorf("JSON = %s, want [\"fra\",\"sfo\"]", string(data))
	}
	var back pq.StringArray
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back) != 2 || back[0] != "fra" || back[1] != "sfo" {
		t.Errorf("round-trip = %v", back)
	}
}

func TestStringArray_EmptyToJSON(t *testing.T) {
	sa := pq.StringArray{}
	data, err := json.Marshal(sa)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != `[]` {
		t.Errorf("JSON = %s, want []", string(data))
	}
}
