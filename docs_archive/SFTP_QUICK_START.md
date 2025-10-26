# SFTP Local Pipeline - Quick Start

## TL;DR

New script for running local staging pipeline with optional SFTP download: `run_local_with_sftp.py`

## Usage

### Without SFTP (Manual Files)

```bash
cd DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

### With SFTP (Automatic Download)

```bash
cd DBT/anais_staging

# One-time setup (Private Key - Recommended)
cat > .env << EOF
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
EOF

# Run pipeline
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

**Authentication:** Supports private key (recommended) or password. See `.env.example` for all options.

## What It Does

1. **Downloads files from SFTP** (if `--use-sftp` provided)
   - Automatically renames: `sirec_20251026.csv` → `sa_sirec.csv`
2. **Loads data into DuckDB**
   - Standardizes column names
   - Handles duplicates and truncation
3. **Runs DBT transformations**
   - Creates views
   - Runs tests

## Key Features

✅ SFTP download (optional)
✅ **Private key authentication** (RSA, Ed25519, ECDSA)
✅ Automatic file renaming
✅ Local DuckDB (no PostgreSQL needed)
✅ Enhanced logging
✅ Works alongside standard pipeline

## Files

- **Script:** `DBT/anais_staging/run_local_with_sftp.py`
- **Config Example:** `DBT/anais_staging/.env.example`
- **Full Guide:** `DBT/anais_staging/SFTP_USAGE_GUIDE.md`
- **Implementation Plan:** `SFTP_LOCAL_PIPELINE_PLAN.md`
- **Logs:** `DBT/anais_staging/logs/log_local_sftp.log`

## Comparison

| Command | SFTP Download | Database | When to Use |
|---------|---------------|----------|-------------|
| `uv run -m pipeline.main --env "local" --profile "Staging"` | ❌ Manual files only | DuckDB | Standard local pipeline |
| `uv run -m pipeline.main --env "anais" --profile "Staging"` | ✅ Automatic | PostgreSQL | Production (VM) |
| `uv run run_local_with_sftp.py --use-sftp` | ✅ Automatic | DuckDB | **Local with latest files** |

## Need Help?

- **Full documentation:** See `SFTP_USAGE_GUIDE.md`
- **Troubleshooting:** Check `logs/log_local_sftp.log`
- **CSV validation:** Run `../../validate_csv_schemas.py`
