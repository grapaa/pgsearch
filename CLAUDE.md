# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgsearch is a hybrid RAG (Retrieval-Augmented Generation) tool that indexes PDF/TXT documents into PostgreSQL with pgvector, then searches using combined vector similarity and full-text search. The UI and text processing are in Norwegian.

## Build & Run

```bash
# Install (editable mode)
pip install -e .

# Run the CLI (interactive REPL menu)
pgsearch
```

No tests, linting, or CI are configured.

## Architecture

The application has two pipelines sharing a database and embedding service:

**Indexing:** `cli.py` → `indexer.py` → `extractor.py` → `chunker.py` → `embedding.py` → `database.py`
- Scans a directory for .pdf/.txt files, skipping already-indexed documents and `.ocr.txt` files
- PDF extraction is two-pass: native PyMuPDF text first, EasyOCR fallback for blank/scanned pages
- Text is split into 1000-char chunks with 200-char overlap, paragraph-boundary aware
- Chunks are embedded via Azure OpenAI (`text-embedding-3-small`, 1536 dimensions) and stored with metadata

**Search:** `cli.py` → `searcher.py` → `embedding.py` + `database.py`
- Query is embedded, then hybrid search combines vector cosine similarity + Norwegian full-text search
- Results are ranked using Reciprocal Rank Fusion (RRF) with k=60

## Key Design Details

- **Database schema:** Single table `document_chunks` with HNSW index on embeddings, GIN indexes on tsvector and jsonb metadata. The tsvector column is `GENERATED ALWAYS` using the `'norwegian'` dictionary.
- **Document identity:** Tracked by filename (not full path) via `document_id`. Unique constraint on `(document_id, chunk_index)` with upsert on conflict.
- **Config:** All settings loaded from `.env` via `python-dotenv`. Required vars: `PGSEARCH_DB_CONNECTION`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`. Optional: `AZURE_OPENAI_DEPLOYMENT`.
- **Connection string format:** Standard libpq key=value pairs (e.g., `host=localhost dbname=pgsearch user=postgres password=postgres`).
