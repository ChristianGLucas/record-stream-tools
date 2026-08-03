package nodes

import (
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func sniff(t *testing.T, text string) *gen.SniffFormatResult {
	t.Helper()
	res, err := SniffFormat(nil, nil, &gen.SniffFormatInput{Text: text})
	if err != nil {
		t.Fatalf("SniffFormat returned error: %v", err)
	}
	return res
}

func TestSniffFormat_JsonArray(t *testing.T) {
	res := sniff(t, `[{"a":1},{"a":2}]`)
	if res.GetFormat() != "json_array" {
		t.Errorf("expected json_array, got %q (reason: %s)", res.GetFormat(), res.GetReason())
	}
	if res.GetConfidence() <= 0.5 {
		t.Errorf("expected reasonably high confidence, got %v", res.GetConfidence())
	}
}

func TestSniffFormat_Ndjson(t *testing.T) {
	res := sniff(t, "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n")
	if res.GetFormat() != "ndjson" {
		t.Errorf("expected ndjson, got %q (reason: %s)", res.GetFormat(), res.GetReason())
	}
}

func TestSniffFormat_Csv(t *testing.T) {
	res := sniff(t, "name,age,city\nalice,30,ny\nbob,25,sf\ncarol,40,la\n")
	if res.GetFormat() != "csv" {
		t.Errorf("expected csv, got %q (reason: %s)", res.GetFormat(), res.GetReason())
	}
}

func TestSniffFormat_Sse(t *testing.T) {
	res := sniff(t, "event: update\nid: 1\ndata: hello\n\ndata: world\n\n")
	if res.GetFormat() != "sse" {
		t.Errorf("expected sse, got %q (reason: %s)", res.GetFormat(), res.GetReason())
	}
}

func TestSniffFormat_LogLinesFallback(t *testing.T) {
	res := sniff(t, "2026-08-02 12:00:00 server started\nlistening on :8080\n")
	if res.GetFormat() != "log_lines" {
		t.Errorf("expected log_lines fallback, got %q (reason: %s)", res.GetFormat(), res.GetReason())
	}
}

func TestSniffFormat_EmptyInputIsUnknown(t *testing.T) {
	res := sniff(t, "")
	if res.GetFormat() != "unknown" {
		t.Errorf("expected unknown, got %q", res.GetFormat())
	}
	if res.GetConfidence() != 0 {
		t.Errorf("expected zero confidence for empty input, got %v", res.GetConfidence())
	}
}

func TestSniffFormat_WhitespaceOnlyIsUnknown(t *testing.T) {
	res := sniff(t, "   \n\t  ")
	if res.GetFormat() != "unknown" {
		t.Errorf("expected unknown, got %q", res.GetFormat())
	}
}

func TestSniffFormat_ConfidenceAlwaysInRange(t *testing.T) {
	inputs := []string{
		`[1,2,3]`,
		"a\nb\nc\n",
		"x,y\n1,2\n",
		"data: hi\n\n",
		"",
		"just one line of prose",
	}
	for _, in := range inputs {
		res := sniff(t, in)
		if res.GetConfidence() < 0 || res.GetConfidence() > 1 {
			t.Errorf("input %q: confidence %v out of [0,1] range", in, res.GetConfidence())
		}
		if res.GetReason() == "" {
			t.Errorf("input %q: expected a non-empty reason", in)
		}
	}
}
