package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// ParseJsonArrayItems parses a WHOLE JSON document (bounded, already in
// memory), whose top-level value must be an array, in one call, returning
// its elements re-assembled as one JSON array. Unlike ParseCsvRecords /
// ParseNdjsonRecords, a structural problem here is a whole-document error,
// not a per-item skip: a JSON array element has no cheap resync boundary
// once the decoder's position is untrustworthy past a malformed token,
// matching StreamJsonArrayItems' identical "unrecoverable mid-document"
// behavior. See ParseCsvRecords' doc for why a bounded unary sibling exists
// alongside the Stream* pipeline node.
func ParseJsonArrayItems(ctx context.Context, ax axiom.Context, input *gen.JsonArrayParseInput) (*gen.JsonArrayParseResult, error) {
	dec := json.NewDecoder(strings.NewReader(stripBOM(input.GetText())))

	tok, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Empty or whitespace-only document: "nothing to report", not a
			// structural problem -- matches StreamJsonArrayItems' identical
			// treatment of a frameless/blank input.
			return &gen.JsonArrayParseResult{ItemsJson: "[]"}, nil
		}
		return &gen.JsonArrayParseResult{
			ItemsJson: "[]",
			Error:     &gen.JsonArrayParseError{Code: "PARSE_ERROR", Message: err.Error()},
		}, nil
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return &gen.JsonArrayParseResult{
			ItemsJson: "[]",
			Error:     &gen.JsonArrayParseError{Code: "INVALID_ARGUMENT", Message: "top-level JSON value is not an array"},
		}, nil
	}

	var items []json.RawMessage
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return &gen.JsonArrayParseResult{
				ItemsJson: "[]",
				Error:     &gen.JsonArrayParseError{Code: "PARSE_ERROR", Message: err.Error()},
			}, nil
		}
		items = append(items, raw)
	}

	itemsJSON := []byte("[]")
	if items != nil {
		if b, err := json.Marshal(items); err == nil {
			itemsJSON = b
		}
	}

	return &gen.JsonArrayParseResult{
		ItemsJson:   string(itemsJSON),
		ItemsParsed: int32(len(items)),
	}, nil
}
