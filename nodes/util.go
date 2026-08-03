package nodes

import "strings"

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
