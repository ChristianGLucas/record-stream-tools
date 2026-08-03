package nodes

import (
	"strings"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func collectNdjson(t *testing.T, text string) []*gen.NdjsonRecordFrame {
	t.Helper()
	in := make(chan *gen.NdjsonInput, 1)
	in <- &gen.NdjsonInput{Text: text}
	close(in)

	var frames []*gen.NdjsonRecordFrame
	if err := StreamNdjsonRecords(nil, nil, in, func(f *gen.NdjsonRecordFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamNdjsonRecords returned error: %v", err)
	}
	return frames
}

func TestStreamNdjsonRecords_OrderingAndCounts(t *testing.T) {
	frames := collectNdjson(t, "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	for i, f := range frames {
		if f.GetIndex() != int32(i) {
			t.Errorf("frame %d: expected index %d, got %d", i, i, f.GetIndex())
		}
		if f.GetLineNumber() != int32(i+1) {
			t.Errorf("frame %d: expected line_number %d, got %d", i, i+1, f.GetLineNumber())
		}
		if f.GetIsError() {
			t.Errorf("frame %d: unexpected is_error", i)
		}
		isLast := i == len(frames)-1
		if f.GetIsFinal() != isLast {
			t.Errorf("frame %d: expected is_final=%v, got %v", i, isLast, f.GetIsFinal())
		}
	}
	if frames[0].GetJson() != `{"a":1}` {
		t.Errorf("frame 0: expected json %q, got %q", `{"a":1}`, frames[0].GetJson())
	}
	if frames[2].GetJson() != `{"a":3}` {
		t.Errorf("frame 2: expected json %q, got %q", `{"a":3}`, frames[2].GetJson())
	}
}

func TestStreamNdjsonRecords_BlankLinesSkipped(t *testing.T) {
	frames := collectNdjson(t, "{\"a\":1}\n\n\n{\"a\":2}\n")
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames (blank lines skipped), got %d", len(frames))
	}
	if frames[0].GetIndex() != 0 || frames[1].GetIndex() != 1 {
		t.Errorf("expected contiguous indices 0,1 despite skipped blanks; got %d,%d", frames[0].GetIndex(), frames[1].GetIndex())
	}
	// line numbers still reflect the ORIGINAL document position.
	if frames[1].GetLineNumber() != 4 {
		t.Errorf("expected frame 1 line_number 4, got %d", frames[1].GetLineNumber())
	}
}

func TestStreamNdjsonRecords_MidStreamMalformedLineSkipsAndContinues(t *testing.T) {
	frames := collectNdjson(t, "{\"a\":1}\nnot json\n{\"a\":3}\n")
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames (bad line reported, not dropped), got %d", len(frames))
	}
	if frames[0].GetIsError() {
		t.Errorf("frame 0 should not be an error")
	}
	if !frames[1].GetIsError() {
		t.Errorf("frame 1 should be an error frame")
	}
	if frames[1].GetErrorMessage() == "" {
		t.Errorf("expected a non-empty error_message on the malformed frame")
	}
	if frames[1].GetJson() != "" {
		t.Errorf("expected empty json on an error frame, got %q", frames[1].GetJson())
	}
	if frames[1].GetIsFinal() {
		t.Errorf("malformed middle frame must not be final")
	}
	if frames[2].GetIsError() {
		t.Errorf("frame 2 (after the bad line) should parse fine")
	}
	if !frames[2].GetIsFinal() {
		t.Errorf("expected last frame to be final")
	}
}

func TestStreamNdjsonRecords_EmptyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectNdjson(t, "")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for empty input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Errorf("expected the single frame to be is_final")
	}
	if frames[0].GetIsError() || frames[0].GetJson() != "" {
		t.Errorf("expected a zero-value non-error frame for empty input")
	}
}

func TestStreamNdjsonRecords_WhitespaceOnlyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectNdjson(t, "   \n\t\n   ")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for whitespace-only input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Errorf("expected the single frame to be is_final")
	}
}

func TestStreamNdjsonRecords_NonObjectJsonValuesAreValid(t *testing.T) {
	// NDJSON lines may be any JSON value, not just objects.
	frames := collectNdjson(t, "42\n\"hello\"\ntrue\nnull\n[1,2,3]\n")
	if len(frames) != 5 {
		t.Fatalf("expected 5 frames, got %d", len(frames))
	}
	want := []string{"42", `"hello"`, "true", "null", "[1,2,3]"}
	for i, w := range want {
		if frames[i].GetIsError() {
			t.Errorf("frame %d: unexpected error %q", i, frames[i].GetErrorMessage())
		}
		if frames[i].GetJson() != w {
			t.Errorf("frame %d: expected json %q, got %q", i, w, frames[i].GetJson())
		}
	}
}

func TestStreamNdjsonRecords_CRLFLineEndings(t *testing.T) {
	frames := collectNdjson(t, "{\"a\":1}\r\n{\"a\":2}\r\n")
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].GetJson() != `{"a":1}` {
		t.Errorf("expected clean json with no stray \\r, got %q", frames[0].GetJson())
	}
}

func TestStreamNdjsonRecords_BOMStripped(t *testing.T) {
	frames := collectNdjson(t, "\uFEFF{\"a\":1}\n")
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].GetIsError() {
		t.Fatalf("expected BOM to be stripped so the first line parses; got error %q", frames[0].GetErrorMessage())
	}
	if frames[0].GetJson() != `{"a":1}` {
		t.Errorf("expected json %q, got %q", `{"a":1}`, frames[0].GetJson())
	}
}

// collectNdjsonMulti feeds text as SEVERAL input frames (one per element of
// chunks) rather than one, exercising the v0.2.0 stateful multi-frame path.
func collectNdjsonMulti(t *testing.T, chunks []string) []*gen.NdjsonRecordFrame {
	t.Helper()
	in := make(chan *gen.NdjsonInput, len(chunks))
	for _, c := range chunks {
		in <- &gen.NdjsonInput{Text: c}
	}
	close(in)

	var frames []*gen.NdjsonRecordFrame
	if err := StreamNdjsonRecords(nil, nil, in, func(f *gen.NdjsonRecordFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamNdjsonRecords returned error: %v", err)
	}
	return frames
}

func TestStreamNdjsonRecords_RecordSpansChunkBoundary(t *testing.T) {
	// The middle record's line is split mid-token across two frames.
	chunks := []string{
		"{\"a\":1}\n{\"a\":2, \"b\":",
		"\"tail\"}\n{\"a\":3}\n",
	}
	frames := collectNdjsonMulti(t, chunks)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d: %+v", len(frames), frames)
	}
	for i, f := range frames {
		if f.GetIsError() {
			t.Fatalf("frame %d unexpectedly an error: %q", i, f.GetErrorMessage())
		}
	}
	if frames[1].GetJson() != `{"a":2, "b":"tail"}` {
		t.Errorf("expected the split record reassembled, got %q", frames[1].GetJson())
	}
	if !frames[2].GetIsFinal() {
		t.Error("expected last frame final")
	}

	// Cross-check against the same document delivered as a single N=1 frame:
	// splitting into frames must never change the parsed result.
	whole := chunks[0] + chunks[1]
	n1 := collectNdjson(t, whole)
	if len(n1) != len(frames) {
		t.Fatalf("N=1 vs multi-frame frame count differs: %d vs %d", len(n1), len(frames))
	}
	for i := range n1 {
		if n1[i].GetJson() != frames[i].GetJson() || n1[i].GetLineNumber() != frames[i].GetLineNumber() {
			t.Errorf("frame %d differs between N=1 and multi-frame: %+v vs %+v", i, n1[i], frames[i])
		}
	}
}

func TestStreamNdjsonRecords_UTF8RuneSplitAcrossChunks(t *testing.T) {
	// "café" JSON-encoded as a string; split the multi-byte 'é' (0xC3 0xA9)
	// across two frames — StreamBody-style byte-aligned, not rune-aligned,
	// chunking. The split lands strictly inside the JSON string content, not
	// on the delimiting '\n', which per lineSplitter's design is the only
	// byte it ever treats specially.
	full := `{"name":"café"}` + "\n"
	splitAt := strings.Index(full, "\xc3") + 1 // split between the two bytes of 'é'
	chunks := []string{full[:splitAt], full[splitAt:]}

	frames := collectNdjsonMulti(t, chunks)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d: %+v", len(frames), frames)
	}
	if frames[0].GetIsError() {
		t.Fatalf("unexpected error (rune corrupted across chunk boundary): %q", frames[0].GetErrorMessage())
	}
	if frames[0].GetJson() != `{"name":"café"}` {
		t.Errorf("expected café preserved intact, got %q", frames[0].GetJson())
	}
}

func TestStreamNdjsonRecords_TailWithoutTrailingNewlineFlushMultiFrame(t *testing.T) {
	chunks := []string{"{\"a\":1}\n{\"a\":2}", ""} // no trailing newline; delivered as 2 frames
	frames := collectNdjsonMulti(t, chunks)
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames (tail flushed without trailing newline), got %d: %+v", len(frames), frames)
	}
	if frames[1].GetJson() != `{"a":2}` || !frames[1].GetIsFinal() {
		t.Errorf("expected tail record flushed as final, got %+v", frames[1])
	}
}

func TestStreamNdjsonRecords_EmptyInputChannelYieldsNoFrames(t *testing.T) {
	in := make(chan *gen.NdjsonInput)
	close(in)
	var frames []*gen.NdjsonRecordFrame
	if err := StreamNdjsonRecords(nil, nil, in, func(f *gen.NdjsonRecordFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expected 0 frames for an empty request channel, got %d", len(frames))
	}
}
