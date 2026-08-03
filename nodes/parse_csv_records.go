package nodes

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// ParseCsvRecords parses a WHOLE CSV document (bounded, already in memory)
// in one call into a JSON array of records, one object per data row keyed
// by column name, every value a plain string. It reuses the exact same
// encoding/csv engine as StreamCsvRecords (RFC 4180 quoting, embedded
// newlines, CRLF/LF, leading BOM), but differs deliberately in two ways:
//
//  1. It returns ONE message, not a stream of frames — for composing with a
//     JSON-records-based downstream node (e.g. a reconciliation tool) inside
//     a unary flow, where StreamCsvRecords' per-row pipeline frames cannot
//     be accumulated into a single message for such a consumer.
//  2. A malformed or ragged (wrong column count) row is SKIPPED and
//     reported in skipped_rows rather than aborting the whole parse — since
//     Go's encoding/csv.Reader resyncs at the next line after most syntax
//     errors (verified: a stray quote or extraneous data after a quoted
//     field only ever costs the one offending row). The one exception is a
//     genuine unterminated quoted field, which can still swallow the rest
//     of the document looking for its closing quote before hitting EOF —
//     see CsvParseResult's doc for that caveat.
func ParseCsvRecords(ctx context.Context, ax axiom.Context, input *gen.CsvParseInput) (*gen.CsvParseResult, error) {
	return parseCsvRecords(
		input.GetText(),
		input.GetDelimiter(),
		input.GetHasHeader(),
		input.GetTrimLeadingSpace(),
		input.GetExplicitHeaders(),
	), nil
}

func parseCsvRecords(text, delimiter string, hasHeader, trimLeadingSpace bool, explicitHeaders []string) *gen.CsvParseResult {
	if len([]rune(delimiter)) > 1 {
		return &gen.CsvParseResult{
			RecordsJson: "[]",
			Error:       &gen.CsvParseError{Code: "INVALID_ARGUMENT", Message: "delimiter must be a single character"},
		}
	}

	cr := csv.NewReader(strings.NewReader(stripBOM(text)))
	cr.FieldsPerRecord = -1 // ragged rows are handled explicitly below, not rejected by the reader
	cr.TrimLeadingSpace = trimLeadingSpace
	if delimiter != "" {
		cr.Comma = []rune(delimiter)[0]
	}

	var headers []string
	if hasHeader {
		h, err := cr.Read()
		if err != nil && err != io.EOF {
			return &gen.CsvParseResult{
				RecordsJson: "[]",
				Error:       &gen.CsvParseError{Code: "PARSE_ERROR", Message: err.Error()},
			}
		}
		if err == nil {
			headers = h
		}
	}
	if len(explicitHeaders) > 0 {
		headers = explicitHeaders
	}
	if dup, ok := firstDuplicate(headers); ok {
		return &gen.CsvParseResult{
			RecordsJson: "[]",
			Error: &gen.CsvParseError{
				Code:    "INVALID_ARGUMENT",
				Message: fmt.Sprintf("duplicate column name %q in headers -- records cannot be safely keyed by name", dup),
			},
		}
	}

	var records []map[string]string
	var skipped []*gen.CsvSkippedRow
	rowNum := int32(0)

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			skipped = append(skipped, &gen.CsvSkippedRow{RowNumber: rowNum, Reason: err.Error()})
			continue
		}
		if headers == nil {
			// No header row and no explicit_headers: establish column
			// names from the first data row's own width.
			headers = make([]string, len(rec))
			for i := range headers {
				headers[i] = fmt.Sprintf("col_%d", i)
			}
		}
		if len(rec) != len(headers) {
			skipped = append(skipped, &gen.CsvSkippedRow{
				RowNumber: rowNum,
				Reason:    fmt.Sprintf("column count mismatch: expected %d (from headers), got %d", len(headers), len(rec)),
			})
			continue
		}
		row := make(map[string]string, len(headers))
		for i, h := range headers {
			row[h] = rec[i]
		}
		records = append(records, row)
	}

	recordsJSON := []byte("[]")
	if records != nil {
		b, err := json.Marshal(records)
		if err == nil {
			recordsJSON = b
		}
	}

	return &gen.CsvParseResult{
		RecordsJson: string(recordsJSON),
		Headers:     headers,
		RowsParsed:  int32(len(records)),
		SkippedRows: skipped,
	}
}

// firstDuplicate reports the first column name that appears more than once
// in headers. A duplicate column name would silently overwrite an earlier
// value when building each record's map[string]string, so it is rejected
// as a whole-document error rather than allowed to corrupt records.
func firstDuplicate(headers []string) (string, bool) {
	seen := make(map[string]bool, len(headers))
	for _, h := range headers {
		if seen[h] {
			return h, true
		}
		seen[h] = true
	}
	return "", false
}
