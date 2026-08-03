package nodes

import (
	"context"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamLogLines splits raw multi-line text into individual lines and emits
// one LogLineFrame per line, in order, with each line's line terminator
// stripped. Every line, including an empty one, produces a frame — there is
// no malformed-line concept for plain text, so this node has no error path.
func StreamLogLines(ctx context.Context, ax axiom.Context, in <-chan *gen.LogLinesInput, emit func(*gen.LogLineFrame) error) error {
	for input := range in {
		if err := streamLogLinesOne(input, emit); err != nil {
			return err
		}
	}
	return nil
}

func streamLogLinesOne(input *gen.LogLinesInput, emit func(*gen.LogLineFrame) error) error {
	text := stripBOM(input.GetText())
	lines := splitLines(text)

	for i, l := range lines {
		frame := &gen.LogLineFrame{
			Index:      int32(i),
			LineNumber: int32(i + 1),
			Text:       l,
			IsFinal:    i == len(lines)-1,
		}
		if err := emit(frame); err != nil {
			return err
		}
	}
	return nil
}
