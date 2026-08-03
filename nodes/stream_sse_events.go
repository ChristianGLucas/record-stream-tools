package nodes

import (
	"context"
	"strconv"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// sseLineSplitter incrementally extracts lines from a byte stream fed across
// MULTIPLE chunks, treating "\r\n", a lone "\r", or a lone "\n" as ONE line
// terminator per the WHATWG event-stream spec — and correctly handling a
// "\r" that lands at the very end of one feed() with its paired "\n" (if
// any) arriving in the next, never splitting that pair into two lines. This
// is the one node in the package whose line-terminator rule ("\r" alone
// counts) differs from lineSplitter's plain "\n"-only rule, so it gets its
// own splitter rather than reusing lineSplitter.
type sseLineSplitter struct {
	buf    []byte
	heldCR bool
}

func (s *sseLineSplitter) feed(chunk string) []string {
	b := []byte(chunk)
	if s.heldCR {
		s.heldCR = false
		if len(b) > 0 && b[0] == '\n' {
			b = b[1:]
		}
	}
	s.buf = append(s.buf, b...)

	var lines []string
	for {
		n := len(s.buf)
		if n == 0 {
			break
		}
		idx := -1
		termLen := 0
		ambiguous := false
		for i := 0; i < n; i++ {
			c := s.buf[i]
			if c == '\n' {
				idx, termLen = i, 1
				break
			}
			if c == '\r' {
				if i+1 < n {
					if s.buf[i+1] == '\n' {
						idx, termLen = i, 2
					} else {
						idx, termLen = i, 1
					}
				} else {
					ambiguous = true
				}
				break
			}
		}
		if ambiguous {
			// The buffer's LAST byte is a bare "\r" whose follow-up we
			// haven't seen yet: the line before it is complete regardless
			// (a "\r" always starts a terminator), but we defer consuming
			// past it until the next feed()/final() tells us whether a
			// paired "\n" follows.
			lines = append(lines, string(s.buf[:n-1]))
			s.buf = nil
			s.heldCR = true
			break
		}
		if idx < 0 {
			break
		}
		lines = append(lines, string(s.buf[:idx]))
		s.buf = s.buf[idx+termLen:]
	}
	return lines
}

// final flushes whatever remains in the buffer as the last line — mirroring
// the guaranteed trailing segment strings.Split leaves after the final
// terminator, even if empty. Call exactly once, after the input is
// exhausted.
func (s *sseLineSplitter) final() string {
	line := string(s.buf)
	s.buf = nil
	return line
}

// StreamSseEvents parses a raw text/event-stream (Server-Sent Events)
// transcript per the WHATWG SSE specification, as a STREAM of input frames
// making up ONE continuous transcript: field-accumulation state (the
// pending id/event/data/retry buffers) is shared across frames via a single
// generator invocation, so an event's fields may span a chunk boundary, and
// the pending event is dispatched once the input channel closes.
//
// Frame boundaries never affect the result: splitting a transcript into 1
// frame or many produces byte-identical output — the N=1 case behaves
// EXACTLY as 0.1.x. Unrecognized field names and comment lines are silently
// ignored per spec, so there is no error frame for this node — the format is
// total by design. A request channel closed with ZERO frames emits nothing
// (not even a terminal frame) and simply returns.
func StreamSseEvents(ctx context.Context, ax axiom.Context, in <-chan *gen.SseInput, emit func(*gen.SseEventFrame) error) error {
	first, ok := <-in
	if !ok {
		return nil
	}

	var (
		splitter     sseLineSplitter
		dataBuf      strings.Builder
		eventTypeBuf string
		idBuf        string
		retryBuf     int32
		retrySet     bool
		pending      *gen.SseEventFrame
		idx          int32
	)

	dispatch := func() error {
		if dataBuf.Len() == 0 {
			// Per spec: an empty data buffer resets the event type buffer
			// and aborts dispatch without emitting an event.
			eventTypeBuf = ""
			return nil
		}
		data := strings.TrimSuffix(dataBuf.String(), "\n")
		evType := eventTypeBuf
		if evType == "" {
			evType = "message"
		}
		f := &gen.SseEventFrame{
			Index: idx, Id: idBuf, Event: evType, Data: data,
			Retry: retryBuf, RetrySet: retrySet,
		}
		idx++
		dataBuf.Reset()
		eventTypeBuf = ""
		if pending != nil {
			if err := emit(pending); err != nil {
				return err
			}
		}
		pending = f
		return nil
	}

	processLine := func(line string) error {
		if line == "" {
			return dispatch()
		}
		if strings.HasPrefix(line, ":") {
			return nil // comment line, ignored per spec
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
		return nil
	}

	for _, line := range splitter.feed(stripBOM(first.GetText())) {
		if err := processLine(line); err != nil {
			return err
		}
	}
	for input := range in {
		for _, line := range splitter.feed(input.GetText()) {
			if err := processLine(line); err != nil {
				return err
			}
		}
	}
	if err := processLine(splitter.final()); err != nil {
		return err
	}
	if err := dispatch(); err != nil { // flush a pending event even without a trailing blank line
		return err
	}

	if pending == nil {
		return emit(&gen.SseEventFrame{IsFinal: true})
	}
	pending.IsFinal = true
	return emit(pending)
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
