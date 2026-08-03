package nodes

import (
	"bytes"
	"io"
	"strings"
)

// stripBOM removes a leading UTF-8 byte-order mark, if present. Wire-format
// exports (especially from Windows tools) commonly prepend one, and a naive
// line/token scan would otherwise fold it into the first line or field.
func stripBOM(s string) string {
	return strings.TrimPrefix(s, "\uFEFF")
}

// splitLines splits text into lines on "\n", stripping a trailing "\r" from
// each line so both LF and CRLF documents are handled uniformly. Unlike
// strings.Split(text, "\n"), an empty input still yields one empty line
// (matching Split's own behavior: strings.Split("", "\n") == [""]), which is
// the deliberate "empty document is one empty line" contract documented on
// LogLineFrame.
func splitLines(text string) []string {
	lines := strings.Split(text, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	return lines
}

// --- v0.2.0 stateful, chunk-spanning streaming support ---------------------
//
// The helpers below let a node accumulate parser state across MULTIPLE input
// frames of the same document, so records may span a chunk boundary and the
// whole document is processed the SAME way regardless of how it was split
// into frames — feeding it as one frame (N=1) or as many produces
// byte-identical output. They operate on raw bytes throughout (never
// decoding a frame's text independently), so a UTF-8 rune split across a
// chunk boundary is never corrupted: the split only ever lands on a single
// ASCII delimiter byte ('\n'), which per the UTF-8 encoding can never appear
// as part of a multi-byte sequence.

// lineSegment is one raw line extracted from a concatenated byte stream,
// exactly matching strings.Split(fullText, "\n") + per-line
// TrimSuffix(l, "\r") semantics — but computed incrementally across chunk
// boundaries instead of requiring the whole text up front.
type lineSegment struct {
	// lineNumber is the 1-based position of this segment among ALL raw
	// segments extracted so far (including ones a caller may filter out,
	// e.g. NDJSON's blank-line skip) — matches splitLines' i+1 indexing.
	lineNumber int32
	// text is the segment's content with a trailing "\r" stripped.
	text string
}

// lineSplitter incrementally extracts lineSegments from bytes fed via
// feed(), matching strings.Split(fullText, "\n") applied to the FULL
// concatenation of every fed chunk (then per-segment "\r" trimming) —
// regardless of where chunk boundaries fall.
type lineSplitter struct {
	buf        []byte
	lineNumber int32
}

// feed appends chunk to the internal buffer and returns every COMPLETE line
// segment (terminated by "\n") it can now extract; any trailing partial
// segment stays buffered for the next feed() or final().
func (s *lineSplitter) feed(chunk string) []lineSegment {
	s.buf = append(s.buf, chunk...)
	var out []lineSegment
	for {
		i := bytes.IndexByte(s.buf, '\n')
		if i < 0 {
			break
		}
		raw := s.buf[:i]
		s.buf = s.buf[i+1:]
		s.lineNumber++
		out = append(out, lineSegment{lineNumber: s.lineNumber, text: strings.TrimSuffix(string(raw), "\r")})
	}
	return out
}

// final returns the trailing segment after the last "\n" fed (possibly
// empty) — mirroring the guaranteed trailing element strings.Split leaves
// after the final delimiter. Call exactly once, after the input is
// exhausted.
func (s *lineSplitter) final() lineSegment {
	s.lineNumber++
	return lineSegment{lineNumber: s.lineNumber, text: strings.TrimSuffix(string(s.buf), "\r")}
}

// chunkReader adapts a pull function yielding successive byte chunks into an
// io.Reader, so a stdlib streaming parser (encoding/csv, encoding/json) can
// be driven directly from a fed pipeline input channel — chunk boundaries
// need no special handling, since the stdlib reader simply blocks (via the
// next call) exactly as it would reading a real network stream.
type chunkReader struct {
	next    func() (string, bool)
	pending []byte
}

func newChunkReader(next func() (string, bool)) *chunkReader {
	return &chunkReader{next: next}
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		text, ok := r.next()
		if !ok {
			return 0, io.EOF
		}
		r.pending = []byte(text)
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
