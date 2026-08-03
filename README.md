# record-stream-tools

Incremental record-by-record streaming parsers for common wire formats —
NDJSON/JSONL, CSV, JSON arrays, raw log lines, and Server-Sent Events. Every
node in this package is an Axiom **pipeline (generator) node**: instead of
buffering a whole document and returning it as one blob, it emits one typed
frame per record as soon as that record is parsed, so a consumer sees
progressive results on large inputs.

## Use it from your agent or app

Every node in this package is a **live, auto-scaling API endpoint** on the
[Axiom](https://axiomide.com) marketplace — call it from an AI agent or your
own code, with nothing to self-host.

**📦 See it on the marketplace:**
https://dev.axiomide.com/marketplace/christiangeorgelucas/record-stream-tools@0.1.0

**Hook it up to an AI agent (MCP).** Add Axiom's hosted MCP server to any MCP
client and every node becomes a typed tool your agent can call — search the
catalog, inspect a schema, and invoke it directly.

```bash
# Claude Code
claude mcp add --transport http axiom https://api.axiomide.com/mcp \
  --header "Authorization: Bearer $AXIOM_API_KEY"
```

Claude Desktop, Cursor, or any config-based client:

```json
{
  "mcpServers": {
    "axiom": {
      "type": "http",
      "url": "https://api.axiomide.com/mcp",
      "headers": { "Authorization": "Bearer YOUR_AXIOM_API_KEY" }
    }
  }
}
```

**Call it from the CLI.**

```bash
axiom invoke christiangeorgelucas/record-stream-tools/StreamNdjsonRecords --input '{"text": "{\"a\":1}\n{\"a\":2}\n"}'
```

**Call it over HTTP** (streaming — pipeline nodes emit progressive `data:`
frames over Server-Sent Events, not one buffered response):

```bash
curl -N -H 'Accept: text/event-stream' \
  -H "Authorization: Bearer $AXIOM_API_KEY" \
  -H 'Content-Type: application/json' \
  -X POST https://api.axiomide.com/v1/nodes/christiangeorgelucas/record-stream-tools/0.1.0/StreamNdjsonRecords \
  -d '{"text": "{\"a\":1}\n{\"a\":2}\n"}'
```

### Get started free

Install the CLI:

```bash
# macOS / Linux — Homebrew
brew install axiomide/tap/axiom

# macOS / Linux — install script
curl -fsSL https://raw.githubusercontent.com/AxiomIDE/axiom-releases/main/install.sh | sh
```

**Windows:** download the `windows/amd64` `.zip` from the
[releases page](https://github.com/AxiomIDE/axiom-releases/releases), unzip
it, and put `axiom.exe` on your `PATH`.

Then `axiom version` to verify, `axiom login` (GitHub or Google) to
authenticate, and create an API key under **Console → API Keys**. Docs and
sign-up at **[axiomide.com](https://axiomide.com)**.

## Nodes

All five `Stream*` nodes are pipeline (generator) nodes: they take one input
document and emit a sequence of frames, the last of which carries
`is_final: true` (the platform's own transport-level terminal frame is
empty, so `is_final` is the business-level end-of-stream signal). `SniffFormat`
is a plain unary node.

- **StreamNdjsonRecords** — newline-delimited JSON (NDJSON/JSONL) text → one
  frame per record. A malformed line is reported as an error frame and
  skipped; parsing continues (NDJSON lines are independent of one another).
- **StreamCsvRecords** — CSV text → one frame per data row, as both an
  ordered `values` list and (with `has_header: true`) a column-name →value
  `fields` map. Handles RFC 4180 quoting, embedded newlines inside quoted
  fields, CRLF/LF, and a leading BOM. A row-level syntax error is
  unrecoverable mid-document, so it is reported as a single error frame and
  parsing stops.
- **StreamJsonArrayItems** — a JSON document whose top-level value is an
  array → one frame per element, decoded with a true token-by-token
  `encoding/json` `Decoder` (never a full `Unmarshal` of the whole array), so
  a large array streams progressively. A structural problem (not an array,
  or a malformed element) is reported as a single error frame and parsing
  stops.
- **StreamLogLines** — raw multi-line text → one frame per line, with its
  1-based line number and terminator stripped. There is no malformed-line
  concept for plain text.
- **StreamSseEvents** — a raw `text/event-stream` transcript → one frame per
  dispatched event (`id`/`event`/`data`/`retry`), following the WHATWG SSE
  parsing algorithm exactly (including that `id`/`retry` persist across
  events until overwritten, and unrecognized fields/comment lines are
  silently ignored). The format is total by spec, so this node has no error
  frame.
- **SniffFormat** — a small unary companion: given a piece of text, guesses
  which of the above formats it's most likely written in (advisory only, not
  a validator), to help route it to the right `Stream*` node.

## License

MIT — see [LICENSE](LICENSE). Built for the [Axiom](https://axiomide.com)
marketplace.
