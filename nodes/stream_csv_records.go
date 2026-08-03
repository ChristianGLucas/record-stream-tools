package nodes

import (
	"context"
	"encoding/csv"
	"io"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamCsvRecords parses CSV text as a STREAM of input frames making up ONE
// continuous document: it drives Go's encoding/csv.Reader directly off the
// input channel (via a small io.Reader adapter that blocks for the next
// frame exactly as it would reading a real network stream), so RFC 4180
// quoting — including a quoted field's embedded newline — is handled
// correctly even when the quote's closing character arrives in a LATER
// frame than its opening character. CRLF/LF line endings and a leading BOM
// are handled by the standard library, not re-derived here.
//
// has_header / delimiter / trim_leading_space are read ONLY from the FIRST
// frame received; the same fields on any later frame are ignored (a
// document's dialect cannot change mid-stream). Frame boundaries never
// affect the result: splitting a document into 1 frame or many produces
// byte-identical output — the N=1 case behaves EXACTLY as 0.1.x.
//
// A row-level syntax error (e.g. an unterminated quoted field) is
// unrecoverable mid-document — byte alignment after such an error is not
// well-defined — so it is reported as a single error frame with
// is_final=true and parsing stops for that input. A request channel closed
// with ZERO frames emits nothing (not even a terminal frame) and simply
// returns.
func StreamCsvRecords(ctx context.Context, ax axiom.Context, in <-chan *gen.CsvInput, emit func(*gen.CsvRecordFrame) error) error {
	first, ok := <-in
	if !ok {
		return nil
	}
	delimiter := first.GetDelimiter()
	hasHeader := first.GetHasHeader()
	trimLeadingSpace := first.GetTrimLeadingSpace()
	firstText := stripBOM(first.GetText())

	consumedFirst := false
	reader := newChunkReader(func() (string, bool) {
		if !consumedFirst {
			consumedFirst = true
			return firstText, true
		}
		v, ok := <-in
		if !ok {
			return "", false
		}
		return v.GetText(), true
	})

	return streamCsv(reader, delimiter, hasHeader, trimLeadingSpace, emit)
}

func streamCsv(r io.Reader, delimiter string, hasHeader, trimLeadingSpace bool, emit func(*gen.CsvRecordFrame) error) error {
	if len([]rune(delimiter)) > 1 {
		return emit(&gen.CsvRecordFrame{
			IsError:      true,
			ErrorMessage: "delimiter must be a single character",
			IsFinal:      true,
		})
	}

	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // a row with a different column count is not an error
	cr.TrimLeadingSpace = trimLeadingSpace
	if delimiter != "" {
		cr.Comma = []rune(delimiter)[0]
	}

	var header []string
	if hasHeader {
		h, err := cr.Read()
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
	cur, curErr := cr.Read()
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

		next, nextErr := cr.Read()
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
