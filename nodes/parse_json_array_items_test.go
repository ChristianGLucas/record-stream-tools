package nodes

import (
	"encoding/json"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func parseJsonArray(t *testing.T, input *gen.JsonArrayParseInput) *gen.JsonArrayParseResult {
	t.Helper()
	res, err := ParseJsonArrayItems(nil, nil, input)
	if err != nil {
		t.Fatalf("ParseJsonArrayItems returned error: %v", err)
	}
	return res
}

func TestParseJsonArrayItems_Basics(t *testing.T) {
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: `[{"n":1},{"n":2},"three"]`})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetItemsParsed() != 3 {
		t.Fatalf("expected items_parsed=3, got %d", res.GetItemsParsed())
	}
	var out []json.RawMessage
	if err := json.Unmarshal([]byte(res.GetItemsJson()), &out); err != nil {
		t.Fatalf("items_json did not decode: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 items, got %d", len(out))
	}
}

func TestParseJsonArrayItems_EmptyArray(t *testing.T) {
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: `[]`})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetItemsParsed() != 0 || res.GetItemsJson() != "[]" {
		t.Errorf("expected empty result, got %+v", res)
	}
}

func TestParseJsonArrayItems_NotAnArrayIsError(t *testing.T) {
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: `{"a":1}`})
	if res.GetError() == nil || res.GetError().GetCode() != "INVALID_ARGUMENT" {
		t.Fatalf("expected INVALID_ARGUMENT, got %+v", res.GetError())
	}
	if res.GetItemsJson() != "[]" {
		t.Errorf("expected empty items_json on error, got %q", res.GetItemsJson())
	}
}

func TestParseJsonArrayItems_MalformedElementIsWholeDocumentError(t *testing.T) {
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: `[{"n":1}, {bad json}]`})
	if res.GetError() == nil || res.GetError().GetCode() != "PARSE_ERROR" {
		t.Fatalf("expected PARSE_ERROR, got %+v", res.GetError())
	}
}

func TestParseJsonArrayItems_EmptyDocumentIsValidEmptyNotError(t *testing.T) {
	// Matches StreamJsonArrayItems' identical treatment of blank input as
	// "nothing to report" rather than a structural problem.
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: ""})
	if res.GetError() != nil {
		t.Fatalf("expected no error for empty document, got %+v", res.GetError())
	}
	if res.GetItemsParsed() != 0 || res.GetItemsJson() != "[]" {
		t.Errorf("expected empty result, got %+v", res)
	}
}

func TestParseJsonArrayItems_WhitespaceOnlyDocumentIsValidEmptyNotError(t *testing.T) {
	res := parseJsonArray(t, &gen.JsonArrayParseInput{Text: "   \n\t "})
	if res.GetError() != nil {
		t.Fatalf("expected no error for whitespace-only document, got %+v", res.GetError())
	}
	if res.GetItemsParsed() != 0 || res.GetItemsJson() != "[]" {
		t.Errorf("expected empty result, got %+v", res)
	}
}
