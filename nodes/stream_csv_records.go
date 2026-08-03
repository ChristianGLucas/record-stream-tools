package nodes

import (
	"context"
	"encoding/csv"
	"io"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamCsvRecords parses CSV text and emits one CsvRecordFrame per data
// row, in document order, using Go's encoding/csv (RFC 4180 quoting,
// embedded newlines inside quoted fields, and both CRLF/LF line endings are
// all handled by the standard library, not re-derived here). A row-level
// syntax error (e.g. an unterminated quoted field) is unrecoverable
// mid-document — byte alignment after such an error is not well-defined —
// so it is reported as a single error frame with is_final=true and parsing
// stops for that input.
func StreamCsvRecords(ctx context.Context, ax axiom.Context, in <-chan *gen.CsvInput, emit func(*gen.CsvRecordFrame) error) error {
	for input := range in {
		if err := streamCsvOne(input, emit); err != nil {
			return err
		}
	}
	return nil
}

func streamCsvOne(input *gen.CsvInput, emit func(*gen.CsvRecordFrame) error) error {
	text := stripBOM(input.GetText())

	if delim := input.GetDelimiter(); len([]rune(delim)) > 1 {
		return emit(&gen.CsvRecordFrame{
			IsError:      true,
			ErrorMessage: "delimiter must be a single character",
			IsFinal:      true,
		})
	}

	r := csv.NewReader(strings.NewReader(text))
	r.FieldsPerRecord = -1 // a row with a different column count is not an error
	r.TrimLeadingSpace = input.GetTrimLeadingSpace()
	if delim := input.GetDelimiter(); delim != "" {
		r.Comma = []rune(delim)[0]
	}

	var header []string
	if input.GetHasHeader() {
		h, err := r.Read()
		if err == io.EOF {
			// header-only-or-empty document: nothing to stream.
			return emit(&gen.CsvRecordFrame{IsFinal: true})
		}
		if err != nil {
			return emit(&gen.CsvRecordFrame{
				IsError:      true,
				ErrorMessage: err.Error(),
				IsFinal:      true,
			})
		}
		header = h
	}

	// 1-row lookahead: we only know a frame is the last one once the next
	// Read() call tells us there is nothing more.
	cur, curErr := r.Read()
	rowNum := int32(1)
	idx := int32(0)
	if curErr == io.EOF {
		return emit(&gen.CsvRecordFrame{IsFinal: true})
	}

	for {
		if curErr != nil {
			return emit(&gen.CsvRecordFrame{
				Index:        idx,
				RowNumber:    rowNum,
				IsError:      true,
				ErrorMessage: curErr.Error(),
				IsFinal:      true,
			})
		}

		next, nextErr := r.Read()
		isFinal := nextErr == io.EOF

		frame := &gen.CsvRecordFrame{
			Index:     idx,
			RowNumber: rowNum,
			Values:    cur,
			IsFinal:   isFinal,
		}
		if header != nil {
			fields := make(map[string]string, len(header))
			for i, col := range header {
				if i < len(cur) {
					fields[col] = cur[i]
				} else {
					fields[col] = ""
				}
			}
			frame.Fields = fields
		}
		if err := emit(frame); err != nil {
			return err
		}
		if isFinal {
			return nil
		}

		cur, curErr = next, nextErr
		idx++
		rowNum++
	}
}
