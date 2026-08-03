package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamJsonArrayItems tokenizes a JSON document whose top-level value is
// expected to be an array, as a STREAM of input frames making up ONE
// continuous document: it drives encoding/json's token-level Decoder
// directly off the input channel (via a small io.Reader adapter that blocks
// for the next frame exactly as it would reading a real network stream), so
// an element may span a chunk boundary. Elements are decoded and emitted
// one at a time — a large array streams progressively instead of the first
// frame waiting on the entire document to parse.
//
// Frame boundaries never affect the result: splitting a document into 1
// frame or many produces byte-identical output — the N=1 case behaves
// EXACTLY as 0.1.x. A structural problem — the top-level value is not an
// array, or an element fails to parse — is unrecoverable mid-document (the
// decoder's position is no longer trustworthy past a malformed token), so it
// is reported as a single error frame with is_final=true and parsing stops.
// A request channel closed with ZERO frames emits nothing (not even a
// terminal frame) and simply returns.
func StreamJsonArrayItems(ctx context.Context, ax axiom.Context, in <-chan *gen.JsonArrayInput, emit func(*gen.JsonArrayItemFrame) error) error {
	first, ok := <-in
	if !ok {
		return nil
	}
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

	return streamJsonArray(reader, emit)
}

func streamJsonArray(r io.Reader, emit func(*gen.JsonArrayItemFrame) error) error {
	dec := json.NewDecoder(r)
	tok, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			// No token at all: empty or whitespace-only document — nothing
			// to stream, not an error.
			return emit(&gen.JsonArrayItemFrame{IsFinal: true})
		}
		return emit(&gen.JsonArrayItemFrame{IsError: true, ErrorMessage: err.Error(), IsFinal: true})
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return emit(&gen.JsonArrayItemFrame{
			IsError:      true,
			ErrorMessage: "top-level JSON value is not an array",
			IsFinal:      true,
		})
	}

	idx := int32(0)
	readNext := func() (*gen.JsonArrayItemFrame, bool) {
		if !dec.More() {
			return nil, false
		}
		var raw json.RawMessage
		f := &gen.JsonArrayItemFrame{Index: idx}
		idx++
		if err := dec.Decode(&raw); err != nil {
			f.IsError = true
			f.ErrorMessage = err.Error()
		} else {
			f.Json = string(raw)
		}
		return f, true
	}

	cur, ok := readNext()
	if !ok {
		// Valid empty array: "[]".
		return emit(&gen.JsonArrayItemFrame{IsFinal: true})
	}

	for {
		if cur.GetIsError() {
			cur.IsFinal = true
			return emit(cur)
		}
		next, nextOk := readNext()
		cur.IsFinal = !nextOk
		if err := emit(cur); err != nil {
			return err
		}
		if !nextOk {
			return nil
		}
		cur = next
	}
}
