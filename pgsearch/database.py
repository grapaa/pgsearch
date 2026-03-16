import json
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from pgvector.psycopg import register_vector


class DatabaseService:
    def __init__(self, connection_string: str):
        self._conninfo = connection_string

    @contextmanager
    def _connection(self, autocommit: bool = True):
        """Context manager that always closes the connection on exit."""
        conn = psycopg.connect(self._conninfo, autocommit=autocommit, connect_timeout=10)
        register_vector(conn)
        try:
            yield conn
        except Exception:
            if not autocommit:
                conn.rollback()
            raise
        else:
            if not autocommit:
                conn.commit()
        finally:
            conn.close()

    def setup(self) -> None:
        with self._connection() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS byggesaker (
                    saksnr          TEXT PRIMARY KEY,
                    metadata        jsonb NOT NULL DEFAULT '{}'
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS document_chunks (
                    id              SERIAL PRIMARY KEY,
                    document_id     TEXT NOT NULL,
                    chunk_index     INTEGER NOT NULL,
                    content         TEXT NOT NULL,
                    embedding       vector(1536),
                    content_tsv     tsvector
                        GENERATED ALWAYS AS (to_tsvector('norwegian', content)) STORED,
                    metadata        jsonb NOT NULL DEFAULT '{}',
                    saksnr          TEXT REFERENCES byggesaker(saksnr),
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (document_id, chunk_index)
                );
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_embedding_hnsw ON document_chunks
                    USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_content_tsv ON document_chunks
                    USING gin (content_tsv);
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_chunks_metadata ON document_chunks
                    USING gin (metadata);
            """)

    def upsert_byggesaker(self, saker: list[dict]) -> None:
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO byggesaker (saksnr, metadata)
                    VALUES (%(saksnr)s, %(metadata)s::jsonb)
                    ON CONFLICT (saksnr) DO UPDATE SET
                        metadata = EXCLUDED.metadata;
                    """,
                    [
                        {
                            "saksnr": sak["saksnr"],
                            "metadata": json.dumps(sak["metadata"]),
                        }
                        for sak in saker
                    ],
                )

    def insert_chunks(self, chunks: list[dict]) -> None:
        # autocommit=False: all chunks for a document are committed atomically.
        # If the process crashes mid-batch, nothing is stored and the document
        # will be retried on the next run.
        with self._connection(autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO document_chunks (document_id, chunk_index, content, embedding, metadata, saksnr)
                    VALUES (%(document_id)s, %(chunk_index)s, %(content)s, %(embedding)s, %(metadata)s::jsonb, %(saksnr)s)
                    ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        saksnr = EXCLUDED.saksnr;
                    """,
                    [
                        {
                            "document_id": c["document_id"],
                            "chunk_index": c["chunk_index"],
                            "content": c["content"],
                            "embedding": c.get("embedding"),
                            "metadata": json.dumps(c.get("metadata", {})),
                            "saksnr": c.get("saksnr"),
                        }
                        for c in chunks
                    ],
                )

    def hybrid_search(
        self,
        query_embedding: list[float],
        query_text: str,
        top_k: int = 10,
        candidate_limit: int = 50,
    ) -> list[dict]:
        with self._connection() as conn:
            conn.row_factory = dict_row
            return conn.execute(
                """
                WITH vector_search AS (
                    SELECT id, document_id, chunk_index, content, metadata, saksnr,
                           1 - (embedding <=> %(embedding)s::vector) AS vector_score,
                           ROW_NUMBER() OVER (ORDER BY embedding <=> %(embedding)s::vector) AS vector_rank
                    FROM document_chunks
                    ORDER BY embedding <=> %(embedding)s::vector
                    LIMIT %(candidate_limit)s
                ),
                fts_search AS (
                    SELECT id, document_id, chunk_index, content, metadata, saksnr,
                           ts_rank_cd(content_tsv, plainto_tsquery('norwegian', %(query_text)s)) AS fts_score,
                           ROW_NUMBER() OVER (
                               ORDER BY ts_rank_cd(content_tsv, plainto_tsquery('norwegian', %(query_text)s)) DESC
                           ) AS fts_rank
                    FROM document_chunks
                    WHERE content_tsv @@ plainto_tsquery('norwegian', %(query_text)s)
                    ORDER BY fts_score DESC
                    LIMIT %(candidate_limit)s
                ),
                meta_search AS (
                    SELECT dc.id, dc.document_id, dc.chunk_index, dc.content, dc.metadata, dc.saksnr,
                           ts_rank_cd(
                               to_tsvector('norwegian',
                                   COALESCE(b.metadata->>'beskrivelse', '') || ' ' ||
                                   COALESCE(b.metadata->>'gnr_bnr', '') || ' ' ||
                                   COALESCE(b.metadata->>'saksbehandler', '') || ' ' ||
                                   COALESCE(b.metadata->>'avsender_mottaker', '') || ' ' ||
                                   COALESCE(b.saksnr, '') || ' ' ||
                                   COALESCE(dc.document_id, '')
                               ),
                               plainto_tsquery('norwegian', %(query_text)s)
                           ) AS meta_score,
                           ROW_NUMBER() OVER (
                               ORDER BY ts_rank_cd(
                                   to_tsvector('norwegian',
                                       COALESCE(b.metadata->>'beskrivelse', '') || ' ' ||
                                       COALESCE(b.metadata->>'gnr_bnr', '') || ' ' ||
                                       COALESCE(b.metadata->>'saksbehandler', '') || ' ' ||
                                       COALESCE(b.metadata->>'avsender_mottaker', '') || ' ' ||
                                       COALESCE(b.saksnr, '') || ' ' ||
                                       COALESCE(dc.document_id, '')
                                   ),
                                   plainto_tsquery('norwegian', %(query_text)s)
                               ) DESC
                           ) AS meta_rank
                    FROM document_chunks dc
                    JOIN byggesaker b ON b.saksnr = dc.saksnr
                    WHERE to_tsvector('norwegian',
                              COALESCE(b.metadata->>'beskrivelse', '') || ' ' ||
                              COALESCE(b.metadata->>'gnr_bnr', '') || ' ' ||
                              COALESCE(b.metadata->>'saksbehandler', '') || ' ' ||
                              COALESCE(b.metadata->>'avsender_mottaker', '') || ' ' ||
                              COALESCE(b.saksnr, '') || ' ' ||
                              COALESCE(dc.document_id, '')
                          ) @@ plainto_tsquery('norwegian', %(query_text)s)
                    ORDER BY meta_score DESC
                    LIMIT %(candidate_limit)s
                ),
                all_candidates AS (
                    SELECT id, document_id, chunk_index, content, metadata, saksnr,
                           vector_score, 0::real AS fts_score, 0::real AS meta_score,
                           vector_rank, NULL::bigint AS fts_rank, NULL::bigint AS meta_rank
                    FROM vector_search
                    UNION ALL
                    SELECT id, document_id, chunk_index, content, metadata, saksnr,
                           0, fts_score, 0,
                           NULL, fts_rank, NULL
                    FROM fts_search
                    UNION ALL
                    SELECT id, document_id, chunk_index, content, metadata, saksnr,
                           0, 0, meta_score,
                           NULL, NULL, meta_rank
                    FROM meta_search
                ),
                combined AS (
                    SELECT
                        id,
                        MAX(document_id) AS document_id,
                        MAX(chunk_index) AS chunk_index,
                        MAX(content) AS content,
                        MAX(metadata::text)::jsonb AS metadata,
                        MAX(saksnr) AS saksnr,
                        MAX(vector_score) AS vector_score,
                        MAX(fts_score) AS fts_score,
                        MAX(meta_score) AS meta_score,
                        COALESCE(0.4 / (60 + MIN(vector_rank)), 0)
                            + COALESCE(0.2 / (60 + MIN(fts_rank)), 0)
                            + COALESCE(0.4 / (60 + MIN(meta_rank)), 0) AS rrf_score
                    FROM all_candidates
                    GROUP BY id
                )
                SELECT * FROM combined
                ORDER BY rrf_score DESC
                LIMIT %(top_k)s;
                """,
                {
                    "embedding": query_embedding,
                    "query_text": query_text,
                    "candidate_limit": candidate_limit,
                    "top_k": top_k,
                },
            ).fetchall()

    def get_statistics(self) -> dict:
        try:
            with self._connection() as conn:
                row = conn.execute("""
                    SELECT COUNT(DISTINCT document_id), COUNT(*)
                    FROM document_chunks;
                """).fetchone()
                return {"documents": row[0], "chunks": row[1]}
        except psycopg.errors.UndefinedTable:
            raise RuntimeError("Databasen er ikke satt opp. Kjør 'Sett opp database' (valg 1) først.")

    def get_indexed_document_ids(self) -> set[str]:
        try:
            with self._connection() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT document_id FROM document_chunks;"
                ).fetchall()
                return {row[0] for row in rows}
        except psycopg.errors.UndefinedTable:
            raise RuntimeError("Databasen er ikke satt opp. Kjør 'Sett opp database' (valg 1) først.")

    def delete_document(self, document_id: str) -> int:
        with self._connection() as conn:
            cur = conn.execute(
                "DELETE FROM document_chunks WHERE document_id = %(doc_id)s;",
                {"doc_id": document_id},
            )
            return cur.rowcount
