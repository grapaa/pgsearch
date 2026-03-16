# Setup Guide

## Prerequisites

1. **PostgreSQL 15+** with the `pgvector` extension
2. **Python 3.11+**
3. **Azure OpenAI** account with a deployed `text-embedding-3-small` model
4. **NVIDIA GPU** (optional but recommended — EasyOCR OCR fallback runs much faster with CUDA)

---

## 1. PostgreSQL with pgvector

### Option A: Docker (recommended)

```powershell
docker run -d --name pgsearch-db `
  -e POSTGRES_USER=postgres `
  -e POSTGRES_PASSWORD=postgres `
  -e POSTGRES_DB=pgsearch `
  -p 5432:5432 `
  pgvector/pgvector:pg16
```

### Option B: Local PostgreSQL

Install pgvector for your PostgreSQL version:

- **Windows:** Download from https://github.com/pgvector/pgvector/releases
- **Ubuntu/Debian:** `sudo apt install postgresql-16-pgvector`
- **macOS (Homebrew):** `brew install pgvector`

Then create the database:

```sql
CREATE DATABASE pgsearch;
```

---

## 2. Environment variables

Create a `.env` file in the project root:

```env
# Required
PGSEARCH_DB_CONNECTION=host=localhost dbname=pgsearch user=postgres password=postgres

# Required — Azure OpenAI
AZURE_OPENAI_ENDPOINT=https://<your-resource>.openai.azure.com/
AZURE_OPENAI_API_KEY=<your-api-key>

# Optional — defaults to text-embedding-3-small
AZURE_OPENAI_DEPLOYMENT=text-embedding-3-small
```

> The connection string uses libpq key=value format, not a URL.

---

## 3. Install and run

```bash
pip install -e .
pgsearch
```

**Run option 1 (Sett opp database) first** — this creates the `pgvector` extension, tables, and indexes.

---

## 4. GPU support for OCR (optional)

EasyOCR is used as a fallback for scanned PDFs with no text layer. It defaults to GPU if CUDA is available.

Check your CUDA version:
```powershell
nvidia-smi
```

Install PyTorch with the matching CUDA version (replace `cu124` with your version, e.g. `cu121`):
```bash
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124
```

---

## Schema (created automatically)

| Table | Purpose |
|-------|---------|
| `byggesaker` | Case metadata (saksnr + jsonb) |
| `document_chunks` | Text chunks with embeddings and Norwegian FTS |

Indexes: HNSW on embeddings, GIN on tsvector, GIN on jsonb metadata.

---

## Logs

Pipeline runs are logged to `logs/pgsearch.log.YYYY-MM-DD` (daily rotation, 90 days retained). The `logs/` directory is gitignored.
