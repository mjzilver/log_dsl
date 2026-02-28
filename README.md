# Log Query DSL

This project provides a small, efficient log ingestion and query system focused on simple, composable queries over structured JSON log lines.

## Features

- Real-time log ingestion: a background reader tails the log and ingests new JSON lines as they appear.
- Indexed fields: in-memory indices for `level`, tokenized `words`, and `timestamps` to make queries fast.
- Persistent indices: indices are periodically serialized and compressed 
- Metadata persistence: last-read offset and id are stored
- Concurrency: background tasks (reader, index writer, CLI).
- Robust error handling: unified `LogQueryError`
- Simple interactive CLI

## TODO:

- Boolean operators and grouping (AND / OR / NOT).
- Wildcards and prefix/suffix matching.
- Phrase and regex search.
- Pagination / streaming.
- Make ingestion accept any NDJSON input (file, pipe, socket).
