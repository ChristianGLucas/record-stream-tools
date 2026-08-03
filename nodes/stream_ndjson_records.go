package nodes

import (
	"context"
	"encoding/json"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamNdjsonRecords parses newline-delimited JSON (NDJSON/JSONL) as a
// STREAM of input frames making up ONE continuous document: parser state
// (the byte accumulator, the running line number) is shared across frames
// via a single generator invocation, a record's line may span a chunk
// boundary, and the tail is flushed once the input channel closes.
//
// Frame boundaries never affect the result: splitting a document into 1
// frame or many produces byte-identical output. In particular the N=1 case
// (the whole document in one frame) behaves EXACTLY as 0.1.x — see
// NdjsonInput for the concatenation contract across frames. A malformed
// line is reported as a per-line error frame and skipped (lines are
// independent); parsing continues with the next line. The last frame
// emitted has is_final=true. A request channel closed with ZERO frames
// emits nothing (not even a terminal frame) and simply returns.
func StreamNdjsonRecords(ctx context.Context, ax axiom.Context, in <-chan *gen.NdjsonInput, emit func(*gen.NdjsonRecordFrame) error) error {
	first, ok := <-in
	if !ok {
		return nil
	}

	var splitter lineSplitter
	var pending *gen.NdjsonRecordFrame
	idx := int32(0)

	build := func(seg lineSegment) *gen.NdjsonRecordFrame {
		f := &gen.NdjsonRecordFrame{Index: idx, LineNumber: seg.lineNumber}
		idx++
		var raw json.RawMessage
		if err := json.Unmarshal([]byte(seg.text), &raw); err != nil {
			f.IsError = true
			f.ErrorMessage = err.Error()
		} else {
			f.Json = string(raw)
		}
		return f
	}

	// process handles one raw line segment: blank lines never become
	// candidates (NDJSON skips them); otherwise it holds the segment as
	// "pending" until either another segment arrives (proving pending is
	// NOT the last frame) or the stream ends (final() below).
	process := func(seg lineSegment) error {
		if strings.TrimSpace(seg.text) == "" {
			return nil
		}
		f := build(seg)
		if pending != nil {
			if err := emit(pending); err != nil {
				return err
			}
		}
		pending = f
		return nil
	}

	for _, seg := range splitter.feed(stripBOM(first.GetText())) {
		if err := process(seg); err != nil {
			return err
		}
	}
	for input := range in {
		for _, seg := range splitter.feed(input.GetText()) {
			if err := process(seg); err != nil {
				return err
			}
		}
	}
	if err := process(splitter.final()); err != nil {
		return err
	}

	if pending == nil {
		return emit(&gen.NdjsonRecordFrame{IsFinal: true})
	}
	pending.IsFinal = true
	return emit(pending)
}
