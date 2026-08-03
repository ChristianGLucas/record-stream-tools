package nodes

import (
	"context"
	"strconv"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamSseEvents parses a raw text/event-stream transcript per the WHATWG
// SSE "parsing an event stream" algorithm and emits one SseEventFrame per
// dispatched event, in document order. Field lines (data/event/id/retry)
// accumulate into per-event buffers; a blank line dispatches the pending
// event. Unrecognized field names and comment lines (starting with ":") are
// silently ignored, exactly as the spec requires, so this node never
// produces an error frame. One deliberate deviation from a live EventSource
// connection: if the transcript doesn't end with a trailing blank line, a
// pending event with non-empty data is still flushed as the final frame
// (a live connection would instead discard it as incomplete) — this makes
// hand-written or truncated transcripts still yield their last event.
func StreamSseEvents(ctx context.Context, ax axiom.Context, in <-chan *gen.SseInput, emit func(*gen.SseEventFrame) error) error {
	for input := range in {
		if err := streamSseOne(input, emit); err != nil {
			return err
		}
	}
	return nil
}

func streamSseOne(input *gen.SseInput, emit func(*gen.SseEventFrame) error) error {
	text := stripBOM(input.GetText())
	// Per spec, a line ending is any of "\r\n", "\r", or "\n" — normalize all
	// three to "\n" before splitting.
	normalized := strings.ReplaceAll(text, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	lines := strings.Split(normalized, "\n")

	var (
		events       []*gen.SseEventFrame
		dataBuf      strings.Builder
		eventTypeBuf string
		idBuf        string
		retryBuf     int32
		retrySet     bool
	)

	dispatch := func() {
		if dataBuf.Len() == 0 {
			// Per spec: an empty data buffer resets the event type buffer
			// and aborts dispatch without emitting an event.
			eventTypeBuf = ""
			return
		}
		data := strings.TrimSuffix(dataBuf.String(), "\n")
		evType := eventTypeBuf
		if evType == "" {
			evType = "message"
		}
		events = append(events, &gen.SseEventFrame{
			Id:       idBuf,
			Event:    evType,
			Data:     data,
			Retry:    retryBuf,
			RetrySet: retrySet,
		})
		dataBuf.Reset()
		eventTypeBuf = ""
	}

	for _, line := range lines {
		if line == "" {
			dispatch()
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment line, ignored per spec
		}

		field, value, hasColon := strings.Cut(line, ":")
		if hasColon {
			value = strings.TrimPrefix(value, " ") // one leading space, per spec
		}

		switch field {
		case "event":
			eventTypeBuf = value
		case "data":
			dataBuf.WriteString(value)
			dataBuf.WriteByte('\n')
		case "id":
			if !strings.ContainsRune(value, 0) {
				idBuf = value
			}
		case "retry":
			if n, ok := parseAsciiDigits(value); ok {
				retryBuf = n
				retrySet = true
			}
		}
	}
	dispatch() // flush a pending event even without a trailing blank line

	if len(events) == 0 {
		return emit(&gen.SseEventFrame{IsFinal: true})
	}
	for i, e := range events {
		e.Index = int32(i)
		e.IsFinal = i == len(events)-1
		if err := emit(e); err != nil {
			return err
		}
	}
	return nil
}

// parseAsciiDigits reports whether s consists of one or more ASCII digits
// (the SSE spec's condition for a valid "retry:" reconnection-time value —
// no sign, no leading/trailing whitespace) and, if so, its parsed value.
func parseAsciiDigits(s string) (int32, bool) {
	if s == "" {
		return 0, false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}
