# Log Query DSL

This project provides a small, efficient log ingestion and query system focused on simple, composable queries over structured JSON log lines (NDJSON).

## Features

- Real-time log ingestion: a background reader tails the log and ingests new JSON lines as they appear.
- Indexed fields: in-memory indices for `level`, tokenized `words`, and `timestamps` to make queries fast.
- Persistent indices: indices are periodically serialized and compressed 
- Metadata persistence: last-read offset is stored
- Concurrency: background tasks (reader, index writer, CLI).
- Robust error handling: unified `LogQueryError`
- Simple interactive CLI
- Operators (AND / OR / NOT).
- EXPLAIN keyword to print AST
- Prefix, Suffix & Contains matching

## Usage

**Valid expressions**

- `selector=value` — simple equality. Examples: `level=warn`, `word=timeout`.
- Spaces around `=` are not allowed.
- Operators: `AND`, `OR`, `NOT` (use explicit operators between expressions).
    - `NOT` is a unary negation. Use as `NOT expr` or combined: `A AND NOT B`.
    - Avoid ambiguous forms like `A NOT B`; write `A AND NOT B` instead.
- Prefix matching with `word=hell^` and suffix with `word=$ello` 
    - Combined `word=lap^ AND word=$top` for laptop 
- Contains rearch `word=~ord` matches word ordinary etc 
- `EXPLAIN` prints the parsed AST when prefixed to a query. Example:

```
EXPLAIN level=warn AND NOT word=hello
```
The output will show the AST used for evaluation (useful for debugging queries).

## TODO:

- Timestamp filtering
- Operator precedence & parentheses
- Phrase and regex search.
- Pagination or streaming. (LIMIT / OFFSET)
- Make ingestion accept any NDJSON input (file, pipe, socket).
