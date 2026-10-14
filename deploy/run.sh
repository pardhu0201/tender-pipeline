#!/usr/bin/env bash
# Cron entry point. Runs one pipeline job inside docker compose, never two at
# once, then embeds whatever it added so search stays in step with the data.
#
#   deploy/run.sh incremental     # new bids since the last run
#   deploy/run.sh full            # the whole listing (weekly)
set -euo pipefail

MODE="${1:-incremental}"
cd "$(dirname "$0")/.."

# A full crawl takes ~80 minutes; if one is still going, skip this slot quietly
# without even starting a container. (The pipeline's own Postgres advisory lock
# is the real guarantee: it also covers runs started by hand or elsewhere.)
exec 9>/tmp/tender-pipeline.lock
if ! flock --nonblock 9; then
    echo "$(date -Is) previous run still in progress - skipping this one"
    exit 0
fi

echo "$(date -Is) starting $MODE run"
docker compose run --rm -T pipeline run --mode "$MODE"

echo "$(date -Is) embedding new and changed tenders"
docker compose run --rm -T api embed
