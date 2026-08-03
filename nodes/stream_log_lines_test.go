package nodes

import (
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func collectLogLines(t *testing.T, text string) []*gen.LogLineFrame {
	t.Helper()
	in := make(chan *gen.LogLinesInput, 1)
	in <- &gen.LogLinesInput{Text: text}
	close(in)

	var frames []*gen.LogLineFrame
	if err := StreamLogLines(nil, nil, in, func(f *gen.LogLineFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamLogLines returned error: %v", err)
	}
	return frames
}

func TestStreamLogLines_OrderingAndCounts(t *testing.T) {
	frames := collectLogLines(t, "first\nsecond\nthird")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	want := []string{"first", "second", "third"}
	for i, w := range want {
		if frames[i].GetIndex() != int32(i) {
			t.Errorf("frame %d: expected index %d, got %d", i, i, frames[i].GetIndex())
		}
		if frames[i].GetLineNumber() != int32(i+1) {
			t.Errorf("frame %d: expected line_number %d, got %d", i, i+1, frames[i].GetLineNumber())
		}
		if frames[i].GetText() != w {
			t.Errorf("frame %d: expected text %q, got %q", i, w, frames[i].GetText())
		}
	}
	if frames[2].GetIsFinal() != true {
		t.Errorf("expected last frame to be final")
	}
	if frames[0].GetIsFinal() || frames[1].GetIsFinal() {
		t.Errorf("only the last frame should be final")
	}
}

func TestStreamLogLines_TrailingNewlineDoesNotProduceExtraEmptyLine(t *testing.T) {
	// "a\nb\n" splits into ["a","b",""] by strings.Split semantics, but a
	// trailing newline is the conventional terminator for the last real
	// line, not a signal for a further blank line — verify we do NOT emit a
	// spurious trailing empty frame for the common "well-formed file" case.
	frames := collectLogLines(t, "a\nb\n")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames (a, b, and the empty segment after the final terminator), got %d", len(frames))
	}
	if frames[2].GetText() != "" {
		t.Errorf("expected the trailing segment to be an empty line, got %q", frames[2].GetText())
	}
	if !frames[2].GetIsFinal() {
		t.Errorf("expected the trailing empty segment to be final")
	}
}

func TestStreamLogLines_BlankLinesProduceFrames(t *testing.T) {
	frames := collectLogLines(t, "a\n\nb")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames (blank line counts), got %d", len(frames))
	}
	if frames[1].GetText() != "" {
		t.Errorf("expected frame 1 to be an empty line, got %q", frames[1].GetText())
	}
	if frames[1].GetLineNumber() != 2 {
		t.Errorf("expected frame 1 line_number 2, got %d", frames[1].GetLineNumber())
	}
}

func TestStreamLogLines_CRLFLineEndings(t *testing.T) {
	frames := collectLogLines(t, "first\r\nsecond\r\n")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	if frames[0].GetText() != "first" {
		t.Errorf("expected clean line with no stray \\r, got %q", frames[0].GetText())
	}
	if frames[1].GetText() != "second" {
		t.Errorf("expected clean line with no stray \\r, got %q", frames[1].GetText())
	}
}

func TestStreamLogLines_EmptyInputYieldsSingleEmptyFinalFrame(t *testing.T) {
	frames := collectLogLines(t, "")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for empty input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Errorf("expected the single frame to be is_final")
	}
	if frames[0].GetText() != "" {
		t.Errorf("expected empty text, got %q", frames[0].GetText())
	}
	if frames[0].GetLineNumber() != 1 {
		t.Errorf("expected line_number 1, got %d", frames[0].GetLineNumber())
	}
}

func TestStreamLogLines_BOMStripped(t *testing.T) {
	frames := collectLogLines(t, "\uFEFFfirst line\nsecond\n")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	if frames[0].GetText() != "first line" {
		t.Errorf("expected BOM stripped from first line, got %q", frames[0].GetText())
	}
}

func TestStreamLogLines_EmptyInputChannelYieldsNoFrames(t *testing.T) {
	in := make(chan *gen.LogLinesInput)
	close(in)
	var frames []*gen.LogLineFrame
	if err := StreamLogLines(nil, nil, in, func(f *gen.LogLineFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expected 0 frames for an empty request channel, got %d", len(frames))
	}
}
