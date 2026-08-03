package nodes

import (
	"encoding/json"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func parseNdjson(t *testing.T, input *gen.NdjsonParseInput) *gen.NdjsonParseResult {
	t.Helper()
	res, err := ParseNdjsonRecords(nil, nil, input)
	if err != nil {
		t.Fatalf("ParseNdjsonRecords returned error: %v", err)
	}
	return res
}

func TestParseNdjsonRecords_Basics(t *testing.T) {
	res := parseNdjson(t, &gen.NdjsonParseInput{Text: "{\"n\":1}\n{\"n\":2}\n"})
	if res.GetRowsParsed() != 2 {
		t.Fatalf("expected rows_parsed=2, got %d", res.GetRowsParsed())
	}
	var out []map[string]interface{}
	if err := json.Unmarshal([]byte(res.GetRecordsJson()), &out); err != nil {
		t.Fatalf("records_json did not decode: %v", err)
	}
	if len(out) != 2 || out[0]["n"] != float64(1) || out[1]["n"] != float64(2) {
		t.Errorf("unexpected records: %#v", out)
	}
}

func TestParseNdjsonRecords_BlankLinesSkippedMalformedLineReportedRestContinue(t *testing.T) {
	res := parseNdjson(t, &gen.NdjsonParseInput{Text: "{\"n\":1}\n\nnot json\n{\"n\":3}\n"})
	if res.GetRowsParsed() != 2 {
		t.Fatalf("expected 2 successfully parsed lines, got %d", res.GetRowsParsed())
	}
	if len(res.GetSkippedRows()) != 1 || res.GetSkippedRows()[0].GetLineNumber() != 3 {
		t.Fatalf("expected exactly 1 skipped line (line 3), got %+v", res.GetSkippedRows())
	}
}

func TestParseNdjsonRecords_PreservesNonObjectScalarValues(t *testing.T) {
	res := parseNdjson(t, &gen.NdjsonParseInput{Text: "\"a string\"\n42\ntrue\nnull\n"})
	if res.GetRowsParsed() != 4 {
		t.Fatalf("expected 4 rows, got %d", res.GetRowsParsed())
	}
	var out []interface{}
	if err := json.Unmarshal([]byte(res.GetRecordsJson()), &out); err != nil {
		t.Fatalf("records_json did not decode: %v", err)
	}
	if out[0] != "a string" || out[1] != float64(42) || out[2] != true || out[3] != nil {
		t.Errorf("unexpected records: %#v", out)
	}
}

func TestParseNdjsonRecords_EmptyDocument(t *testing.T) {
	res := parseNdjson(t, &gen.NdjsonParseInput{Text: ""})
	if res.GetRowsParsed() != 0 || res.GetRecordsJson() != "[]" {
		t.Errorf("expected empty result, got rows_parsed=%d records_json=%q", res.GetRowsParsed(), res.GetRecordsJson())
	}
}
