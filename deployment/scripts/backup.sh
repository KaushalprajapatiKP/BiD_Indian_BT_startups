#!/usr/bin/env bash
pg_dump -h ${DB_HOST:-localhost} -U ${DB_USER:-postgres} ${DB_NAME:-biotech_startups} > backup_$(date +%F).sql
