#!/usr/bin/env bash
# Contract test for GeM's undocumented API: crawl two live pages into the
# scratch database (tenders_test) and fail loudly if the client no longer
# understands the responses. Production data is never touched.
#
# Run it from the machine that runs the pipeline. GeM refuses connections from
# outside India - GitHub's hosted runners included (verified October 2026) - so
# it cannot run on ordinary CI.
set -euo pipefail
cd "$(dirname "$0")/.."

# Same database password docker compose uses (read from .env if it is set there).
password=$(grep -E '^POSTGRES_PASSWORD=' .env 2>/dev/null | tail -1 | cut -d= -f2- | tr -d "\"'" || true)
scratch_db="postgresql://tender:${password:-tender}@db:5432/tenders_test"

smoke() { docker compose run --rm -T -e DATABASE_URL="$scratch_db" pipeline "$@"; }

docker compose up -d --wait db
smoke init-db
smoke run --mode full --max-pages 2
smoke report
echo "smoke test passed: GeM's API still behaves the way the client expects"
