package nodes

import (
	"context"
	"encoding/json"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// ParseNdjsonRecords parses a WHOLE NDJSON/JSONL document (bounded, already
// in memory) in one call into a JSON array collecting every line's own JSON
// value, in document order. Lines are independent of one another, so a
// malformed line is individually skipped and reported in skipped_rows —
// there is no whole-document error case, unlike ParseCsvRecords. See
// ParseCsvRecords' doc for why a bounded unary sibling exists alongside the
// Stream* pipeline node.
func ParseNdjsonRecords(ctx context.Context, ax axiom.Context, input *gen.NdjsonParseInput) (*gen.NdjsonParseResult, error) {
	lines := strings.Split(stripBOM(input.GetText()), "\n")

	var records []json.RawMessage
	var skipped []*gen.NdjsonSkippedLine
	for i, raw := range lines {
		lineNumber := int32(i + 1)
		line := strings.TrimSuffix(raw, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		var v json.RawMessage
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			skipped = append(skipped, &gen.NdjsonSkippedLine{LineNumber: lineNumber, Reason: err.Error()})
			continue
		}
		records = append(records, v)
	}

	recordsJSON := []byte("[]")
	if records != nil {
		if b, err := json.Marshal(records); err == nil {
			recordsJSON = b
		}
	}

	return &gen.NdjsonParseResult{
		RecordsJson: string(recordsJSON),
		RowsParsed:  int32(len(records)),
		SkippedRows: skipped,
	}, nil
}
