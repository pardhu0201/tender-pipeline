#!/usr/bin/env bash
# Copy the current tenders and their embeddings into another Postgres with
# pgvector - e.g. the Neon database behind a public demo of the search API.
# The target's tables are replaced, so point it at a database used for nothing else.
#
#   deploy/publish_snapshot.sh 'postgresql://user:password@host/dbname?sslmode=require'
set -euo pipefail
target="${1:?usage: deploy/publish_snapshot.sh <target postgres url>}"
cd "$(dirname "$0")/.."

# Same schema on the target, then replace its data with ours.
docker compose run --rm -T -e DATABASE_URL="$target" pipeline init-db
docker compose run --rm -T -e DATABASE_URL="$target" api init-db
docker compose exec -T db psql -v ON_ERROR_STOP=1 -q "$target" \
    -c "TRUNCATE tender_embeddings, rejected_records, tenders, scrape_runs RESTART IDENTITY CASCADE"

docker compose exec -T db pg_dump -U tender -d tenders --data-only --no-owner \
        -t scrape_runs -t tenders -t tender_embeddings \
    | docker compose exec -T db psql -v ON_ERROR_STOP=1 -q "$target"

docker compose exec -T db psql -q "$target" -At -c \
    "SELECT 'published ' || (SELECT count(*) FROM tenders) || ' tenders and '
            || (SELECT count(*) FROM tender_embeddings) || ' embeddings'"
