package nodes

import (
	"strings"
	"testing"

	gen "christiangeorgelucas/record-stream-tools/gen"
)

func collectSse(t *testing.T, text string) []*gen.SseEventFrame {
	t.Helper()
	in := make(chan *gen.SseInput, 1)
	in <- &gen.SseInput{Text: text}
	close(in)

	var frames []*gen.SseEventFrame
	if err := StreamSseEvents(nil, nil, in, func(f *gen.SseEventFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamSseEvents returned error: %v", err)
	}
	return frames
}

func TestStreamSseEvents_OrderingAndCounts(t *testing.T) {
	text := "data: first\n\ndata: second\n\ndata: third\n\n"
	frames := collectSse(t, text)
	if len(frames) != 3 {
		t.Fatalf("expected 3 events, got %d", len(frames))
	}
	want := []string{"first", "second", "third"}
	for i, w := range want {
		if frames[i].GetIndex() != int32(i) {
			t.Errorf("frame %d: expected index %d, got %d", i, i, frames[i].GetIndex())
		}
		if frames[i].GetData() != w {
			t.Errorf("frame %d: expected data %q, got %q", i, w, frames[i].GetData())
		}
		if frames[i].GetEvent() != "message" {
			t.Errorf("frame %d: expected default event type 'message', got %q", i, frames[i].GetEvent())
		}
		isLast := i == len(frames)-1
		if frames[i].GetIsFinal() != isLast {
			t.Errorf("frame %d: expected is_final=%v, got %v", i, isLast, frames[i].GetIsFinal())
		}
	}
}

func TestStreamSseEvents_MultiLineData(t *testing.T) {
	// Per spec, multiple "data:" lines before a dispatch are joined with LF.
	text := "data: line one\ndata: line two\ndata: line three\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	want := "line one\nline two\nline three"
	if frames[0].GetData() != want {
		t.Errorf("expected joined data %q, got %q", want, frames[0].GetData())
	}
}

func TestStreamSseEvents_IdEventDataRetryFields(t *testing.T) {
	text := "id: 42\nevent: update\nretry: 3000\ndata: payload\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	f := frames[0]
	if f.GetId() != "42" {
		t.Errorf("expected id 42, got %q", f.GetId())
	}
	if f.GetEvent() != "update" {
		t.Errorf("expected event 'update', got %q", f.GetEvent())
	}
	if f.GetData() != "payload" {
		t.Errorf("expected data 'payload', got %q", f.GetData())
	}
	if f.GetRetry() != 3000 || !f.GetRetrySet() {
		t.Errorf("expected retry=3000 retry_set=true, got retry=%d retry_set=%v", f.GetRetry(), f.GetRetrySet())
	}
}

func TestStreamSseEvents_IdAndRetryPersistAcrossEvents(t *testing.T) {
	// Per spec, the last event ID buffer and the reconnection time buffer
	// are NOT reset on dispatch — they persist until overwritten.
	text := "id: abc\nretry: 5000\ndata: one\n\ndata: two\n\n"
	frames := collectSse(t, text)
	if len(frames) != 2 {
		t.Fatalf("expected 2 events, got %d", len(frames))
	}
	if frames[0].GetId() != "abc" || frames[1].GetId() != "abc" {
		t.Errorf("expected id 'abc' to persist to the second event, got %q then %q", frames[0].GetId(), frames[1].GetId())
	}
	if !frames[1].GetRetrySet() || frames[1].GetRetry() != 5000 {
		t.Errorf("expected retry to persist to the second event, got retry=%d retry_set=%v", frames[1].GetRetry(), frames[1].GetRetrySet())
	}
}

func TestStreamSseEvents_CommentLinesIgnored(t *testing.T) {
	text := ": this is a comment\ndata: hello\n: another comment\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	if frames[0].GetData() != "hello" {
		t.Errorf("expected data 'hello', got %q", frames[0].GetData())
	}
}

func TestStreamSseEvents_UnrecognizedFieldNameIgnored(t *testing.T) {
	text := "foo: bar\ndata: hello\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	if frames[0].GetData() != "hello" {
		t.Errorf("expected data 'hello' unaffected by unrecognized field, got %q", frames[0].GetData())
	}
}

func TestStreamSseEvents_BareFieldNameNoColon(t *testing.T) {
	// A line with no colon at all is the field name with an empty value.
	// "data" alone therefore contributes one empty data line.
	text := "data\ndata\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	if frames[0].GetData() != "\n" {
		t.Errorf("expected data to be two empty lines joined by LF, got %q", frames[0].GetData())
	}
}

func TestStreamSseEvents_EmptyDataBufferDoesNotDispatch(t *testing.T) {
	// A blank line with no preceding "data:" field must NOT produce an event
	// (data buffer is empty, so dispatch aborts per spec).
	text := "event: ping\n\ndata: real\n\n"
	frames := collectSse(t, text)
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 event (the bare 'event: ping' must not dispatch), got %d", len(frames))
	}
	if frames[0].GetData() != "real" {
		t.Errorf("expected the surviving event's data to be 'real', got %q", frames[0].GetData())
	}
}

func TestStreamSseEvents_NoTrailingBlankLineStillFlushesLastEvent(t *testing.T) {
	// Deliberate deviation from a live EventSource: treat EOF like an
	// implicit blank line so a transcript without a trailing blank line
	// still yields its last event.
	text := "data: first\n\ndata: second"
	frames := collectSse(t, text)
	if len(frames) != 2 {
		t.Fatalf("expected 2 events (including the un-terminated trailing one), got %d", len(frames))
	}
	if frames[1].GetData() != "second" {
		t.Errorf("expected trailing event data 'second', got %q", frames[1].GetData())
	}
	if !frames[1].GetIsFinal() {
		t.Errorf("expected trailing event to be final")
	}
}

func TestStreamSseEvents_CRLFLineEndings(t *testing.T) {
	text := "data: first\r\n\r\ndata: second\r\n\r\n"
	frames := collectSse(t, text)
	if len(frames) != 2 {
		t.Fatalf("expected 2 events, got %d", len(frames))
	}
	if frames[0].GetData() != "first" || frames[1].GetData() != "second" {
		t.Errorf("expected clean data with no stray \\r, got %q and %q", frames[0].GetData(), frames[1].GetData())
	}
}

func TestStreamSseEvents_EmptyInputYieldsSingleFinalFrame(t *testing.T) {
	frames := collectSse(t, "")
	if len(frames) != 1 {
		t.Fatalf("expected exactly 1 frame for empty input, got %d", len(frames))
	}
	if !frames[0].GetIsFinal() {
		t.Errorf("expected the single frame to be is_final")
	}
	if frames[0].GetData() != "" || frames[0].GetEvent() != "" {
		t.Errorf("expected a zero-value frame for empty input, got %#v", frames[0])
	}
}

// collectSseMulti feeds text as SEVERAL input frames rather than one,
// exercising the v0.2.0 stateful multi-frame path.
func collectSseMulti(t *testing.T, chunks []string) []*gen.SseEventFrame {
	t.Helper()
	in := make(chan *gen.SseInput, len(chunks))
	for _, c := range chunks {
		in <- &gen.SseInput{Text: c}
	}
	close(in)

	var frames []*gen.SseEventFrame
	if err := StreamSseEvents(nil, nil, in, func(f *gen.SseEventFrame) error {
		frames = append(frames, f)
		return nil
	}); err != nil {
		t.Fatalf("StreamSseEvents returned error: %v", err)
	}
	return frames
}

func TestStreamSseEvents_EventSpansChunkBoundary(t *testing.T) {
	// The "data:" field's value is split mid-line across two frames.
	chunks := []string{"data: hel", "lo\n\ndata: second\n\n"}
	frames := collectSseMulti(t, chunks)
	if len(frames) != 2 {
		t.Fatalf("expected 2 events, got %d: %+v", len(frames), frames)
	}
	if frames[0].GetData() != "hello" {
		t.Errorf("expected the split field value reassembled as %q, got %q", "hello", frames[0].GetData())
	}
	if !frames[1].GetIsFinal() {
		t.Error("expected last frame final")
	}
}

func TestStreamSseEvents_CRLFSplitAcrossChunkBoundary(t *testing.T) {
	// The "\r" of a "\r\n" terminator lands as the LAST byte of one frame,
	// its paired "\n" as the FIRST byte of the next — the exact ambiguous
	// case sseLineSplitter's heldCR state exists for.
	chunks := []string{"data: first\r", "\n\r\ndata: second\r\n\r\n"}
	frames := collectSseMulti(t, chunks)
	if len(frames) != 2 {
		t.Fatalf("expected 2 events, got %d: %+v", len(frames), frames)
	}
	if frames[0].GetData() != "first" || frames[1].GetData() != "second" {
		t.Fatalf("expected clean split data with no stray CR, got %q and %q", frames[0].GetData(), frames[1].GetData())
	}

	// Cross-check against the same transcript delivered as one N=1 frame.
	whole := chunks[0] + chunks[1]
	n1 := collectSse(t, whole)
	if len(n1) != 2 || n1[0].GetData() != "first" || n1[1].GetData() != "second" {
		t.Fatalf("N=1 reference run gave unexpected result: %+v", n1)
	}
}

func TestStreamSseEvents_UTF8RuneSplitAcrossChunks(t *testing.T) {
	full := "data: café\n\n"
	splitAt := strings.Index(full, "\xc3") + 1
	frames := collectSseMulti(t, []string{full[:splitAt], full[splitAt:]})
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(frames), frames)
	}
	if frames[0].GetData() != "café" {
		t.Errorf("expected café preserved intact across the chunk boundary, got %q", frames[0].GetData())
	}
}

func TestStreamSseEvents_BOMStripped(t *testing.T) {
	frames := collectSse(t, "\uFEFFdata: hello\n\n")
	if len(frames) != 1 {
		t.Fatalf("expected 1 event, got %d", len(frames))
	}
	if frames[0].GetData() != "hello" {
		t.Errorf("expected BOM stripped so field name 'data' is recognized, got data %q", frames[0].GetData())
	}
}
