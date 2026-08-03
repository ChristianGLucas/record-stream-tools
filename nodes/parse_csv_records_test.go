package nodes

import (
	"encoding/json"
	"strings"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func parseCsv(t *testing.T, input *gen.CsvParseInput) *gen.CsvParseResult {
	t.Helper()
	res, err := ParseCsvRecords(nil, nil, input)
	if err != nil {
		t.Fatalf("ParseCsvRecords returned error: %v", err)
	}
	return res
}

func decodeRecords(t *testing.T, recordsJSON string) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	if err := json.Unmarshal([]byte(recordsJSON), &out); err != nil {
		t.Fatalf("records_json did not decode as an array of objects: %v\njson: %s", err, recordsJSON)
	}
	return out
}

func TestParseCsvRecords_HeaderBasics(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{
		Text:      "name,age\nalice,30\nbob,25\n",
		HasHeader: true,
	})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 2 {
		t.Fatalf("expected rows_parsed=2, got %d", res.GetRowsParsed())
	}
	if len(res.GetSkippedRows()) != 0 {
		t.Fatalf("expected no skipped rows, got %+v", res.GetSkippedRows())
	}
	wantHeaders := []string{"name", "age"}
	if len(res.GetHeaders()) != 2 || res.GetHeaders()[0] != wantHeaders[0] || res.GetHeaders()[1] != wantHeaders[1] {
		t.Fatalf("unexpected headers: %v", res.GetHeaders())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0]["name"] != "alice" || records[0]["age"] != "30" {
		t.Errorf("record 0: unexpected fields %#v", records[0])
	}
	if _, isString := records[0]["age"].(string); !isString {
		t.Errorf("expected age to decode as a JSON STRING, got %T: %v", records[0]["age"], records[0]["age"])
	}
	if records[1]["name"] != "bob" {
		t.Errorf("record 1: unexpected fields %#v", records[1])
	}
}

func TestParseCsvRecords_NoHeaderSynthesizesColumnNames(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{Text: "alice,30\nbob,25\n"})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if len(res.GetHeaders()) != 2 || res.GetHeaders()[0] != "col_0" || res.GetHeaders()[1] != "col_1" {
		t.Fatalf("expected synthesized col_0/col_1 headers, got %v", res.GetHeaders())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if records[0]["col_0"] != "alice" || records[0]["col_1"] != "30" {
		t.Errorf("unexpected record 0: %#v", records[0])
	}
}

func TestParseCsvRecords_ExplicitHeadersOverrideDocumentHeaderRow(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{
		Text:            "ignored_a,ignored_b\nalice,30\n",
		HasHeader:       true, // consumes+discards the literal header row
		ExplicitHeaders: []string{"person", "years"},
	})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if len(records) != 1 {
		t.Fatalf("expected 1 record (header row consumed, not counted), got %d", len(records))
	}
	if records[0]["person"] != "alice" || records[0]["years"] != "30" {
		t.Errorf("expected explicit_headers as keys, got %#v", records[0])
	}
}

func TestParseCsvRecords_ExplicitHeadersNoHeaderRowInDocument(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{
		Text:            "alice,30\nbob,25\n",
		ExplicitHeaders: []string{"person", "years"},
	})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 2 {
		t.Fatalf("expected both rows parsed (no header row to consume), got %d", res.GetRowsParsed())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if records[0]["person"] != "alice" {
		t.Errorf("unexpected record 0: %#v", records[0])
	}
}

// --- RFC 4180 torture cases -------------------------------------------------

func TestParseCsvRecords_QuotedFieldWithEmbeddedCommaAndNewline(t *testing.T) {
	// The exact "Doe, Jane refund" repro from the parquet-tools bug report.
	text := "date,amount,ref,desc\n" +
		"2026-01-01,100.50,REF1,Widget sale\n" +
		"2026-01-02,200.75,REF2,\"Doe, Jane refund\"\n" +
		"2026-01-03,50.00,REF3,\"multi\nline note\"\n"
	res := parseCsv(t, &gen.CsvParseInput{Text: text, HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if len(records) != 3 {
		t.Fatalf("expected 3 records (embedded comma/newline must not split rows), got %d: %#v", len(records), records)
	}
	if records[1]["desc"] != "Doe, Jane refund" {
		t.Errorf("expected embedded-comma desc preserved verbatim, got %q", records[1]["desc"])
	}
	if records[2]["desc"] != "multi\nline note" {
		t.Errorf("expected embedded-newline desc preserved verbatim, got %q", records[2]["desc"])
	}
}

func TestParseCsvRecords_EscapedQuotesInsideQuotedField(t *testing.T) {
	// RFC 4180 escapes a literal quote as a doubled quote inside a quoted field.
	text := "name,quote\nalice,\"she said \"\"hello\"\"\"\n"
	res := parseCsv(t, &gen.CsvParseInput{Text: text, HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	want := `she said "hello"`
	if records[0]["quote"] != want {
		t.Errorf("expected %q, got %q", want, records[0]["quote"])
	}
}

func TestParseCsvRecords_CRLFAndBOM(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{
		Text:      "\uFEFFname,age\r\nalice,30\r\n",
		HasHeader: true,
	})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if records[0]["age"] != "30" {
		t.Errorf("expected clean field, got %q", records[0]["age"])
	}
	if res.GetHeaders()[0] != "name" {
		t.Errorf("expected BOM stripped from first header, got %q", res.GetHeaders()[0])
	}
}

func TestParseCsvRecords_CustomDelimiter(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{Text: "name;age\nalice;30\n", HasHeader: true, Delimiter: ";"})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if records[0]["age"] != "30" {
		t.Errorf("unexpected: %#v", records[0])
	}
}

func TestParseCsvRecords_InvalidMultiCharDelimiterIsWholeDocumentError(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{Text: "a,b\n1,2\n", Delimiter: "::"})
	if res.GetError() == nil || res.GetError().GetCode() != "INVALID_ARGUMENT" {
		t.Fatalf("expected INVALID_ARGUMENT error, got %+v", res.GetError())
	}
	if res.GetRecordsJson() != "[]" {
		t.Errorf("expected empty records_json on error, got %q", res.GetRecordsJson())
	}
}

// --- decimal-precision-string verbatim survival -----------------------------

func TestParseCsvRecords_AmountsSurviveAsExactStringsNeverFloat(t *testing.T) {
	// Values chosen specifically because they DO NOT round-trip cleanly
	// through float64 (see the filed parquet-tools bug report) -- proving
	// this node never routes a cell through a numeric type.
	amounts := []string{
		"999999999999.99",
		"0.10",
		"100.10",
		"-0.01",
		"123456789012345.67",
		"1234567.895",
		"0.30",
		"1.00000000000000001",
		"99999999999999999999.99",
		"0.123456789012345678901",
	}
	var sb string
	sb = "amount\n"
	for _, a := range amounts {
		sb += a + "\n"
	}
	res := parseCsv(t, &gen.CsvParseInput{Text: sb, HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if len(records) != len(amounts) {
		t.Fatalf("expected %d records, got %d", len(amounts), len(records))
	}
	for i, want := range amounts {
		got, ok := records[i]["amount"].(string)
		if !ok {
			t.Fatalf("record %d: expected amount to decode as a JSON string, got %T", i, records[i]["amount"])
		}
		if got != want {
			t.Errorf("record %d: expected amount %q preserved VERBATIM, got %q", i, want, got)
		}
	}
	// Independently confirm the raw records_json string contains each
	// amount as a quoted JSON string literal, not a bare JSON number.
	for _, want := range amounts {
		if !jsonContainsQuotedString(res.GetRecordsJson(), want) {
			t.Errorf("expected records_json to contain %q as a quoted string, raw json: %s", want, res.GetRecordsJson())
		}
	}
}

func jsonContainsQuotedString(doc, s string) bool {
	quoted, err := json.Marshal(s)
	if err != nil {
		return false
	}
	return strings.Contains(doc, string(quoted))
}

// --- malformed / ragged row skipping ----------------------------------------

func TestParseCsvRecords_BareQuoteMidDocumentSkipsOnlyThatRow(t *testing.T) {
	// A bare quote in an unquoted field is a genuine syntax error that Go's
	// encoding/csv resyncs from at the next line (verified empirically).
	text := "a,b\nalice,30\nbo\"b,25\ncarl,40\n"
	res := parseCsv(t, &gen.CsvParseInput{Text: text, HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected whole-document error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 2 {
		t.Fatalf("expected 2 successfully parsed rows (alice, carl), got %d", res.GetRowsParsed())
	}
	if len(res.GetSkippedRows()) != 1 {
		t.Fatalf("expected exactly 1 skipped row, got %+v", res.GetSkippedRows())
	}
	if res.GetSkippedRows()[0].GetRowNumber() != 2 {
		t.Errorf("expected the skipped row to be data row 2, got %d", res.GetSkippedRows()[0].GetRowNumber())
	}
	if res.GetSkippedRows()[0].GetReason() == "" {
		t.Errorf("expected a non-empty skip reason")
	}
	records := decodeRecords(t, res.GetRecordsJson())
	if records[0]["a"] != "alice" || records[1]["a"] != "carl" {
		t.Errorf("expected alice+carl survived, bob dropped: %#v", records)
	}
}

func TestParseCsvRecords_RaggedRowIsSkippedNotMisassigned(t *testing.T) {
	text := "a,b,c\nalice,30,eng\nbob,25\ncarl,40,ops,extra\n"
	res := parseCsv(t, &gen.CsvParseInput{Text: text, HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 1 {
		t.Fatalf("expected only the well-formed row (alice) to parse, got %d", res.GetRowsParsed())
	}
	if len(res.GetSkippedRows()) != 2 {
		t.Fatalf("expected 2 skipped ragged rows (bob short, carl long), got %+v", res.GetSkippedRows())
	}
}

func TestParseCsvRecords_UnterminatedQuoteHeaderRowIsWholeDocumentError(t *testing.T) {
	text := "\"unterminated\nalice,30\n"
	res := parseCsv(t, &gen.CsvParseInput{Text: text, HasHeader: true})
	if res.GetError() == nil || res.GetError().GetCode() != "PARSE_ERROR" {
		t.Fatalf("expected PARSE_ERROR whole-document error, got %+v", res.GetError())
	}
	if res.GetRecordsJson() != "[]" {
		t.Errorf("expected empty records_json, got %q", res.GetRecordsJson())
	}
}

// --- degenerate documents ---------------------------------------------------

func TestParseCsvRecords_EmptyDocument(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{Text: ""})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 0 || res.GetRecordsJson() != "[]" {
		t.Errorf("expected zero rows and empty array, got rows_parsed=%d records_json=%q", res.GetRowsParsed(), res.GetRecordsJson())
	}
}

func TestParseCsvRecords_HeaderOnlyDocument(t *testing.T) {
	res := parseCsv(t, &gen.CsvParseInput{Text: "name,age\n", HasHeader: true})
	if res.GetError() != nil {
		t.Fatalf("unexpected error: %+v", res.GetError())
	}
	if res.GetRowsParsed() != 0 {
		t.Errorf("expected 0 data rows, got %d", res.GetRowsParsed())
	}
}
