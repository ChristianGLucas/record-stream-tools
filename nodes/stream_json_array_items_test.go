package nodes

import (
	"fmt"
	"strings"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func collectJsonArrayItems(t *testing.T, text string) []*gen.JsonArrayItemFrame {
	t.Helper()
	in := make(chan *gen.JsonArrayInput, 1)
	in <- &gen.JsonArrayInput{Text: text}
	close(in)

	var frames []*gen.JsonArrayItemFrame
	if err := StreamJsonArrayItems(nil, nil, in, func(f *gen.JsonArrayItemFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamJsonArrayItems returned error: %v", err)
	}
	return frames
}

func TestStreamJsonArrayItems_OrderingAndCounts(t *testing.T) {
	frames := collectJsonArrayItems(t, `[{"a":1},{"a":2},{"a":3}]`)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	for i, f := range frames {
		if f.GetIndex() != int32(i) {
			t.Errorf("frame %d: expected index %d, got %d", i, i, f.GetIndex())
		}
		if f.GetIsError() {
			t.Errorf("frame %d: unexpected error %q", i, f.GetErrorMessage())
		}
		isLast := i == len(frames)-1
		if f.GetIsFinal() != isLast {
			t.Errorf("frame %d: expected is_final=%v, got %v", i, isLast, f.GetIsFinal())
		}
	}
	if frames[0].GetJson() != `{"a":1}` {
		t.Errorf("frame 0: expected json %q, got %q", `{"a":1}`, frames[0].GetJson())
	}
}

func TestStreamJsonArrayItems_MixedElementKinds(t *testing.T) {
	frames := collectJsonArrayItems(t, `[1, "two", true, null, [4,5], {"six":6}]`)
	if len(frames) != 6 {
		t.Fatalf("expected 6 frames, got %d", len(frames))
	}
	want := []string{"1", `"two"`, "true", "null", "[4,5]", `{"six":6}`}
	for i, w := range want {
		if frames[i].GetJson() != w {
			t.Errorf("frame %d: expected json %q, got %q", i, w, frames[i].GetJson())
		}
	}
}

func TestStreamJsonArrayItems_EmptyArrayYieldsSingleFinalFrame(t *testing.T) {
	frames := collectJsonArrayItems(t, `[]`)
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for an empty array, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() || frames[0].GetIsError() {
		t.Errorf("expected a single non-error is_final frame, got %#v", frames[0])
	}
}

func TestStreamJsonArrayItems_EmptyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectJsonArrayItems(t, "")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for empty input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() || frames[0].GetIsError() {
		t.Errorf("expected a single non-error is_final frame, got %#v", frames[0])
	}
}

func TestStreamJsonArrayItems_WhitespaceOnlyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectJsonArrayItems(t, "   \n\t  ")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Errorf("expected is_final")
	}
}

func TestStreamJsonArrayItems_TopLevelNotArrayIsStructuralError(t *testing.T) {
	frames := collectJsonArrayItems(t, `{"not":"an array"}`)
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 error frame, got %d", len(frames))
	}
	if !frames[0].GetIsError() || !frames[0].GetIsFinal() {
		t.Errorf("expected a single is_final error frame, got %#v", frames[0])
	}
	if frames[0].GetErrorMessage() == "" {
		t.Errorf("expected a non-empty error_message")
	}
}

func TestStreamJsonArrayItems_UnparsableTopLevelIsStructuralError(t *testing.T) {
	frames := collectJsonArrayItems(t, `not json at all`)
	if len(frames) != 1 || !frames[0].GetIsError() || !frames[0].GetIsFinal() {
		t.Fatalf("expected a single is_final error frame, got %#v", frames)
	}
}

// TestStreamJsonArrayItems_TrueIncrementalDecoding proves the node uses a
// true token-by-token Decoder rather than unmarshaling the whole array up
// front: a naive `json.Unmarshal(text, &[]json.RawMessage{})` implementation
// would fail the ENTIRE array on a single malformed element deep inside and
// emit nothing at all (Unmarshal is all-or-nothing over the whole document).
// The incremental Decoder instead emits every valid element that came
// BEFORE the malformed one as its own good frame, then one error frame —
// proving elements are decoded (and emitted) one at a time, not in a batch.
func TestStreamJsonArrayItems_TrueIncrementalDecoding(t *testing.T) {
	// 5 valid elements, then one syntactically invalid element ({bad} has an
	// unquoted key, which is invalid JSON), then more text that is never
	// reached.
	text := `[1,2,3,4,5,{bad},6,7,8]`
	frames := collectJsonArrayItems(t, text)

	if len(frames) != 6 {
		t.Fatalf("expected 5 good frames + 1 error frame = 6, got %d: %#v", len(frames), frames)
	}
	for i := 0; i < 5; i++ {
		if frames[i].GetIsError() {
			t.Fatalf("frame %d should have decoded successfully before the malformed element, got error %q", i, frames[i].GetErrorMessage())
		}
		want := fmt.Sprintf("%d", i+1)
		if frames[i].GetJson() != want {
			t.Errorf("frame %d: expected json %q, got %q", i, want, frames[i].GetJson())
		}
		if frames[i].GetIsFinal() {
			t.Errorf("frame %d must not be final (an error frame follows)", i)
		}
	}
	last := frames[5]
	if !last.GetIsError() {
		t.Fatalf("expected frame 5 to be the error frame, got %#v", last)
	}
	if !last.GetIsFinal() {
		t.Errorf("expected the error frame to be is_final (elements 6,7,8 must never be reached)")
	}
}

func TestStreamJsonArrayItems_LargeArrayOrderingAtScale(t *testing.T) {
	const n = 5000
	var sb strings.Builder
	sb.WriteByte('[')
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "%d", i)
	}
	sb.WriteByte(']')

	frames := collectJsonArrayItems(t, sb.String())
	if len(frames) != n {
		t.Fatalf("expected %d frames, got %d", n, len(frames))
	}
	for i, f := range frames {
		if f.GetIsError() {
			t.Fatalf("frame %d: unexpected error %q", i, f.GetErrorMessage())
		}
		want := fmt.Sprintf("%d", i)
		if f.GetJson() != want {
			t.Fatalf("frame %d: expected json %q, got %q", i, want, f.GetJson())
		}
	}
	if !frames[n-1].GetIsFinal() {
		t.Errorf("expected last frame to be final")
	}
	for i := 0; i < n-1; i++ {
		if frames[i].GetIsFinal() {
			t.Fatalf("frame %d must not be final", i)
		}
	}
}

// collectJsonArrayItemsMulti feeds text as SEVERAL input frames rather than
// one, exercising the v0.2.0 stateful multi-frame path.
func collectJsonArrayItemsMulti(t *testing.T, chunks []string) []*gen.JsonArrayItemFrame {
	t.Helper()
	in := make(chan *gen.JsonArrayInput, len(chunks))
	for _, c := range chunks {
		in <- &gen.JsonArrayInput{Text: c}
	}
	close(in)

	var frames []*gen.JsonArrayItemFrame
	if err := StreamJsonArrayItems(nil, nil, in, func(f *gen.JsonArrayItemFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamJsonArrayItems returned error: %v", err)
	}
	return frames
}

func TestStreamJsonArrayItems_ElementSpansChunkBoundary(t *testing.T) {
	chunks := []string{`[{"a":1},{"a":2,"b":`, `"tail"},{"a":3}]`}
	frames := collectJsonArrayItemsMulti(t, chunks)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d: %+v", len(frames), frames)
	}
	if frames[1].GetJson() != `{"a":2,"b":"tail"}` {
		t.Errorf("expected the split element reassembled, got %q", frames[1].GetJson())
	}
	if !frames[2].GetIsFinal() {
		t.Error("expected last frame final")
	}

	whole := chunks[0] + chunks[1]
	n1 := collectJsonArrayItems(t, whole)
	if len(n1) != len(frames) {
		t.Fatalf("N=1 vs multi-frame frame count differs: %d vs %d", len(n1), len(frames))
	}
	for i := range n1 {
		if n1[i].GetJson() != frames[i].GetJson() {
			t.Errorf("frame %d differs between N=1 and multi-frame: %q vs %q", i, n1[i].GetJson(), frames[i].GetJson())
		}
	}
}

func TestStreamJsonArrayItems_UTF8RuneSplitAcrossChunks(t *testing.T) {
	full := `["café"]`
	splitAt := strings.Index(full, "\xc3") + 1
	frames := collectJsonArrayItemsMulti(t, []string{full[:splitAt], full[splitAt:]})
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d: %+v", len(frames), frames)
	}
	if frames[0].GetIsError() {
		t.Fatalf("unexpected error (rune corrupted across chunk boundary): %q", frames[0].GetErrorMessage())
	}
	if frames[0].GetJson() != `"café"` {
		t.Errorf("expected café preserved intact, got %q", frames[0].GetJson())
	}
}

func TestStreamJsonArrayItems_EmptyInputChannelYieldsNoFrames(t *testing.T) {
	in := make(chan *gen.JsonArrayInput)
	close(in)
	var frames []*gen.JsonArrayItemFrame
	if err := StreamJsonArrayItems(nil, nil, in, func(f *gen.JsonArrayItemFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("expected 0 frames for an empty request channel, got %d", len(frames))
	}
}

func TestStreamJsonArrayItems_BOMStripped(t *testing.T) {
	frames := collectJsonArrayItems(t, "\uFEFF[1,2]")
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].GetIsError() {
		t.Fatalf("expected BOM stripped so the array parses; got error %q", frames[0].GetErrorMessage())
	}
}
