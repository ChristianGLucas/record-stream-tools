package nodes

import (
	"context"
	"encoding/json"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamNdjsonRecords parses newline-delimited JSON (NDJSON/JSONL) text and
// emits one NdjsonRecordFrame per non-blank line, in document order. A line
// that fails to parse as JSON is reported as an error frame and skipped —
// NDJSON lines are independent of one another, so one bad line never
// invalidates the rest of the document. is_final is set on exactly the last
// frame emitted for each input; an input with zero non-blank lines emits one
// frame with is_final=true and no other data.
func StreamNdjsonRecords(ctx context.Context, ax axiom.Context, in <-chan *gen.NdjsonInput, emit func(*gen.NdjsonRecordFrame) error) error {
	for input := range in {
		if err := streamNdjsonOne(input, emit); err != nil {
			return err
		}
	}
	return nil
}

func streamNdjsonOne(input *gen.NdjsonInput, emit func(*gen.NdjsonRecordFrame) error) error {
	text := stripBOM(input.GetText())

	type candidate struct {
		lineNumber int32
		raw        string
	}
	var lines []candidate
	for i, l := range splitLines(text) {
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, candidate{lineNumber: int32(i + 1), raw: l})
	}

	if len(lines) == 0 {
		return emit(&gen.NdjsonRecordFrame{IsFinal: true})
	}

	for idx, c := range lines {
		frame := &gen.NdjsonRecordFrame{
			Index:      int32(idx),
			LineNumber: c.lineNumber,
			IsFinal:    idx == len(lines)-1,
		}
		var raw json.RawMessage
		if err := json.Unmarshal([]byte(c.raw), &raw); err != nil {
			frame.IsError = true
			frame.ErrorMessage = err.Error()
		} else {
			frame.Json = string(raw)
		}
		if err := emit(frame); err != nil {
			return err
		}
	}
	return nil
}
