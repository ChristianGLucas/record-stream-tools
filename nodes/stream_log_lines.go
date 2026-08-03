package nodes

import (
	"context"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamLogLines splits raw multi-line text into individual lines as a
// STREAM of input frames making up ONE continuous document: the byte
// accumulator and running line number are shared across frames via a single
// generator invocation, so a line may span a chunk boundary, and the tail is
// flushed once the input channel closes.
//
// Frame boundaries never affect the result: splitting a document into 1
// frame or many produces byte-identical output — the N=1 case behaves
// EXACTLY as 0.1.x. Every line, including an empty one, produces a frame —
// there is no malformed-line concept for plain text, so this node has no
// error path. A request channel closed with ZERO frames emits nothing (not
// even a terminal frame) and simply returns.
func StreamLogLines(ctx context.Context, ax axiom.Context, in <-chan *gen.LogLinesInput, emit func(*gen.LogLineFrame) error) error {
	first, ok := <-in
	if !ok {
		return nil
	}

	var splitter lineSplitter
	var pending *gen.LogLineFrame
	idx := int32(0)

	process := func(seg lineSegment) error {
		f := &gen.LogLineFrame{Index: idx, LineNumber: seg.lineNumber, Text: seg.text}
		idx++
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

	pending.IsFinal = true
	return emit(pending)
}
