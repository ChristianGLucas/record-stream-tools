package nodes

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// SniffFormat guesses which of this package's supported wire formats a
// piece of text is most likely written in, using a handful of cheap
// structural checks — never a full parse of the whole document — so a
// caller can route it to the right Stream* node. This is advisory only, not
// a validator: a confident guess can still be wrong on adversarial or
// ambiguous input, and the corresponding Stream* node's own error frame is
// the authoritative parse result.
func SniffFormat(ctx context.Context, ax axiom.Context, input *gen.SniffFormatInput) (*gen.SniffFormatResult, error) {
	text := stripBOM(input.GetText())
	trimmed := strings.TrimSpace(text)

	if trimmed == "" {
		return &gen.SniffFormatResult{Format: "unknown", Confidence: 0, Reason: "input is empty or whitespace-only"}, nil
	}

	nonBlankLines := nonBlankLinesOf(text)

	if strings.HasPrefix(trimmed, "[") && json.Valid([]byte(trimmed)) {
		return &gen.SniffFormatResult{
			Format:     "json_array",
			Confidence: 0.9,
			Reason:     "the full text is valid JSON and its top-level value is an array",
		}, nil
	}

	if sseScore, sseReason := sniffSse(nonBlankLines); sseScore > 0 {
		return &gen.SniffFormatResult{Format: "sse", Confidence: sseScore, Reason: sseReason}, nil
	}

	if len(nonBlankLines) >= 1 {
		validCount := 0
		for _, l := range nonBlankLines {
			if json.Valid([]byte(strings.TrimSpace(l))) {
				validCount++
			}
		}
		if validCount == len(nonBlankLines) {
			if len(nonBlankLines) >= 2 {
				return &gen.SniffFormatResult{
					Format:     "ndjson",
					Confidence: 0.85,
					Reason:     "every non-blank line parses independently as a standalone JSON value",
				}, nil
			}
			return &gen.SniffFormatResult{
				Format:     "ndjson",
				Confidence: 0.4,
				Reason:     "the single non-blank line parses as a standalone JSON value, but there is only one line to corroborate NDJSON framing",
			}, nil
		}
	}

	if csvScore, csvReason := sniffCsv(nonBlankLines); csvScore > 0 {
		return &gen.SniffFormatResult{Format: "csv", Confidence: csvScore, Reason: csvReason}, nil
	}

	if len(nonBlankLines) >= 1 {
		return &gen.SniffFormatResult{
			Format:     "log_lines",
			Confidence: 0.3,
			Reason:     "no structural signal for ndjson/csv/json_array/sse matched; falling back to plain lines of text",
		}, nil
	}

	return &gen.SniffFormatResult{Format: "unknown", Confidence: 0, Reason: "no recognizable structure found"}, nil
}

func nonBlankLinesOf(text string) []string {
	var out []string
	for _, l := range splitLines(text) {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// sniffSse looks for early lines that are SSE field lines (a recognized
// field name followed by ':', or a ':' comment line). A real transcript
// concentrates these in its first few lines.
func sniffSse(nonBlankLines []string) (float64, string) {
	if len(nonBlankLines) == 0 {
		return 0, ""
	}
	sampleLen := len(nonBlankLines)
	if sampleLen > 5 {
		sampleLen = 5
	}
	sseFieldHits, dataHits := 0, 0
	for _, l := range nonBlankLines[:sampleLen] {
		field, _, hasColon := strings.Cut(l, ":")
		if strings.HasPrefix(l, ":") {
			sseFieldHits++
			continue
		}
		if !hasColon {
			continue
		}
		switch field {
		case "data":
			sseFieldHits++
			dataHits++
		case "event", "id", "retry":
			sseFieldHits++
		}
	}
	if dataHits == 0 {
		return 0, ""
	}
	confidence := 0.6 + 0.3*float64(sseFieldHits)/float64(sampleLen)
	if confidence > 0.95 {
		confidence = 0.95
	}
	return confidence, "early lines match SSE field syntax (a 'data:' field is present)"
}

// sniffCsv checks whether the text parses as CSV with a consistent column
// count of at least 2 across every sampled row.
func sniffCsv(nonBlankLines []string) (float64, string) {
	if len(nonBlankLines) == 0 {
		return 0, ""
	}
	r := csv.NewReader(strings.NewReader(strings.Join(nonBlankLines, "\n")))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil || len(rows) == 0 {
		return 0, ""
	}
	cols := len(rows[0])
	if cols < 2 {
		return 0, ""
	}
	consistent := 0
	for _, row := range rows {
		if len(row) == cols {
			consistent++
		}
	}
	ratio := float64(consistent) / float64(len(rows))
	if ratio < 0.8 {
		return 0, ""
	}
	confidence := 0.4 + 0.4*ratio
	return confidence, "text parses as delimited rows with a consistent column count"
}
