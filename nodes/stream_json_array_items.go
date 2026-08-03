package nodes

import (
	"context"
	"encoding/json"
	"strings"

	"christiangeorgelucas/record-stream-tools/axiom"
	gen "christiangeorgelucas/record-stream-tools/gen"
)

// StreamJsonArrayItems tokenizes a JSON document whose top-level value is
// expected to be an array and emits one JsonArrayItemFrame per element, in
// array order. It uses encoding/json's token-level Decoder rather than
// unmarshaling the whole array up front: elements are decoded and emitted
// one at a time, so a large array streams progressively instead of the
// first frame waiting on the entire document to parse. A structural problem
// — the top-level value is not an array, or an element fails to parse — is
// unrecoverable mid-document (the decoder's position is no longer
// trustworthy past a malformed token), so it is reported as a single error
// frame with is_final=true and parsing stops for that input.
func StreamJsonArrayItems(ctx context.Context, ax axiom.Context, in <-chan *gen.JsonArrayInput, emit func(*gen.JsonArrayItemFrame) error) error {
	for input := range in {
		if err := streamJsonArrayOne(input, emit); err != nil {
			return err
		}
	}
	return nil
}

func streamJsonArrayOne(input *gen.JsonArrayInput, emit func(*gen.JsonArrayItemFrame) error) error {
	text := stripBOM(input.GetText())
	if strings.TrimSpace(text) == "" {
		return emit(&gen.JsonArrayItemFrame{IsFinal: true})
	}

	dec := json.NewDecoder(strings.NewReader(text))
	tok, err := dec.Token()
	if err != nil {
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
