package nodes

import (
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func collectCsv(t *testing.T, input *gen.CsvInput) []*gen.CsvRecordFrame {
	t.Helper()
	in := make(chan *gen.CsvInput, 1)
	in <- input
	close(in)

	var frames []*gen.CsvRecordFrame
	if err := StreamCsvRecords(nil, nil, in, func(f *gen.CsvRecordFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamCsvRecords returned error: %v", err)
	}
	return frames
}

func TestStreamCsvRecords_HeaderOrderingAndCounts(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{
		Text:      "name,age\nalice,30\nbob,25\n",
		HasHeader: true,
	})
	if len(frames) != 2 {
		t.Fatalf("expected 2 data-row frames, got %d", len(frames))
	}
	for i, f := range frames {
		if f.GetIndex() != int32(i) {
			t.Errorf("frame %d: expected index %d, got %d", i, i, f.GetIndex())
		}
		if f.GetRowNumber() != int32(i+1) {
			t.Errorf("frame %d: expected row_number %d, got %d", i, i+1, f.GetRowNumber())
		}
		isLast := i == len(frames)-1
		if f.GetIsFinal() != isLast {
			t.Errorf("frame %d: expected is_final=%v, got %v", i, isLast, f.GetIsFinal())
		}
	}
	if frames[0].GetFields()["name"] != "alice" || frames[0].GetFields()["age"] != "30" {
		t.Errorf("frame 0: unexpected fields %#v", frames[0].GetFields())
	}
	if frames[1].GetFields()["name"] != "bob" {
		t.Errorf("frame 1: unexpected fields %#v", frames[1].GetFields())
	}
	if frames[0].GetValues()[0] != "alice" {
		t.Errorf("frame 0: expected values[0]=alice, got %q", frames[0].GetValues()[0])
	}
}

func TestStreamCsvRecords_NoHeaderUsesValues(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{Text: "alice,30\nbob,25\n"})
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if len(frames[0].GetFields()) != 0 {
		t.Errorf("expected no fields map without a header, got %#v", frames[0].GetFields())
	}
	if frames[0].GetValues()[0] != "alice" || frames[0].GetValues()[1] != "30" {
		t.Errorf("unexpected values %#v", frames[0].GetValues())
	}
}

func TestStreamCsvRecords_QuotedFieldWithEmbeddedNewlineAndComma(t *testing.T) {
	text := "name,bio\n\"alice\",\"line one\nline two, still one field\"\nbob,short\n"
	frames := collectCsv(t, &gen.CsvInput{Text: text, HasHeader: true})
	if len(frames) != 2 {
		t.Fatalf("expected 2 data rows (embedded newline must not split a row), got %d", len(frames))
	}
	want := "line one\nline two, still one field"
	if frames[0].GetFields()["bio"] != want {
		t.Errorf("expected bio %q, got %q", want, frames[0].GetFields()["bio"])
	}
	if frames[1].GetFields()["name"] != "bob" {
		t.Errorf("expected row 2 name=bob, got %#v", frames[1].GetFields())
	}
	if !frames[1].GetIsFinal() {
		t.Errorf("expected last row to be final")
	}
}

func TestStreamCsvRecords_CRLFLineEndings(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{
		Text:      "name,age\r\nalice,30\r\nbob,25\r\n",
		HasHeader: true,
	})
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].GetFields()["age"] != "30" {
		t.Errorf("expected clean field with no stray \\r, got %q", frames[0].GetFields()["age"])
	}
}

func TestStreamCsvRecords_BOMStripped(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{
		Text:      "\uFEFFname,age\nalice,30\n",
		HasHeader: true,
	})
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if _, ok := frames[0].GetFields()["name"]; !ok {
		t.Errorf("expected BOM stripped so header key is 'name', got fields %#v", frames[0].GetFields())
	}
}

func TestStreamCsvRecords_CustomDelimiter(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{
		Text:      "name;age\nalice;30\n",
		HasHeader: true,
		Delimiter: ";",
	})
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].GetFields()["age"] != "30" {
		t.Errorf("expected custom delimiter parsed, got fields %#v", frames[0].GetFields())
	}
}

func TestStreamCsvRecords_MalformedRowTerminatesWithErrorFrame(t *testing.T) {
	// An unterminated quoted field is a genuine CSV syntax error.
	text := "name,bio\nalice,\"unterminated\nbob,short\n"
	frames := collectCsv(t, &gen.CsvInput{Text: text, HasHeader: true})
	if len(frames) == 0 {
		t.Fatalf("expected at least one frame")
	}
	last := frames[len(frames)-1]
	if !last.GetIsError() {
		t.Fatalf("expected the last frame to be an error frame, got %#v", last)
	}
	if last.GetErrorMessage() == "" {
		t.Errorf("expected a non-empty error_message")
	}
	if !last.GetIsFinal() {
		t.Errorf("expected the error frame to be is_final")
	}
}

func TestStreamCsvRecords_VariableColumnCountIsNotAnError(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{Text: "a,b,c\nd,e\n"})
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].GetIsError() || frames[1].GetIsError() {
		t.Fatalf("ragged rows must not be reported as errors")
	}
	if len(frames[1].GetValues()) != 2 {
		t.Errorf("expected row 2 to keep its own 2 values, got %#v", frames[1].GetValues())
	}
}

func TestStreamCsvRecords_EmptyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{Text: ""})
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for empty input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() || frames[0].GetIsError() {
		t.Errorf("expected a single non-error is_final frame, got %#v", frames[0])
	}
}

func TestStreamCsvRecords_HeaderOnlyDocumentYieldsSingleFinalFrame(t *testing.T) {
	frames := collectCsv(t, &gen.CsvInput{Text: "name,age\n", HasHeader: true})
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for a header-only document, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() || frames[0].GetIsError() {
		t.Errorf("expected a single non-error is_final frame, got %#v", frames[0])
	}
}
