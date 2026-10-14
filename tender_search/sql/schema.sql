-- Semantic search on top of the pipeline's `tenders` table. Idempotent.
-- Needs the pgvector extension (the docker compose database image ships it).

CREATE EXTENSION IF NOT EXISTS vector;

-- One embedding per tender. `content_hash` is the tender's hash at the time it
-- was embedded, so a tender is re-embedded exactly when its source record changes.
CREATE TABLE IF NOT EXISTS tender_embeddings (
    tender_id     bigint      PRIMARY KEY REFERENCES tenders (id) ON DELETE CASCADE,
    model         text        NOT NULL,
    content_hash  text        NOT NULL,
    embedding     vector(384) NOT NULL,
    embedded_at   timestamptz NOT NULL DEFAULT now()
);

-- Approximate nearest-neighbour index for cosine distance (the <=> operator).
CREATE INDEX IF NOT EXISTS tender_embeddings_hnsw_idx
    ON tender_embeddings USING hnsw (embedding vector_cosine_ops);
