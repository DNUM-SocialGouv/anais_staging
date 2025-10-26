# SFTP Download for Local Pipeline - Usage Guide

## Overview

The `run_local_with_sftp.py` script extends the local staging pipeline to support automatic file downloads from SFTP. This allows you to run the complete pipeline locally (using DuckDB) while pulling the latest data files from the remote SFTP server.

## Why This Script?

The standard `pipeline.main` is installed as a package from the `anais_pipeline` repository and cannot be easily modified. This script provides a **local wrapper** that:

- ✅ Uses the same underlying pipeline code (DuckDBPipeline, SFTPSync)
- ✅ Adds optional SFTP download capability to local execution
- ✅ Works alongside the existing pipeline (doesn't replace it)
- ✅ Requires minimal setup

## Quick Start

### Option 1: Manual Files (No SFTP)

```bash
cd DBT/anais_staging

# 1. Copy CSV files manually
cp ../../ingestion/SIREC/*.csv input/staging/
cp ../../ingestion/SIVSS/*.csv input/staging/

# 2. Rename files to expected format (sa_*.csv)
python3 ../../rename_staging_files.py --execute

# 3. Run pipeline
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

### Option 2: SFTP Download (Automatic)

```bash
cd DBT/anais_staging

# 1. Configure SFTP credentials (one-time setup)
# Option A: Private Key (Recommended)
cat > .env << EOF
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
SFTP_PRIVATE_KEY_PASSPHRASE="passphrase"  # Optional
EOF

# Option B: Password
cat > .env << EOF
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="username"
SFTP_PASSWORD="password"
EOF

# 2. Run pipeline with SFTP
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

## Setup

### Prerequisites

1. **Python environment** with UV package manager
2. **DuckDB** (installed via uv sync)
3. **SFTP credentials** (only needed if using `--use-sftp`)

### Environment Setup (Only for SFTP)

Create a `.env` file in `DBT/anais_staging/` with SFTP credentials.

**Two authentication methods are supported:**

#### Method 1: Private Key Authentication (RECOMMENDED)

```bash
# .env file (DO NOT commit to git!)
SFTP_HOST="your.sftp.server.com"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
SFTP_PRIVATE_KEY_PASSPHRASE="passphrase"  # Optional, only if key is encrypted
```

**Supported key types:** RSA, Ed25519, ECDSA

**Key file permissions:**
```bash
chmod 600 ~/.ssh/id_rsa
```

#### Method 2: Password Authentication

```bash
# .env file (DO NOT commit to git!)
SFTP_HOST="your.sftp.server.com"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PASSWORD="your_password"
```

#### Quick Setup from Example

```bash
cd DBT/anais_staging

# Copy example file
cp .env.example .env

# Edit with your credentials
nano .env  # or vim, code, etc.

# Set restrictive permissions
chmod 600 .env
```

**Authentication Priority:**
1. If `SFTP_PRIVATE_KEY_PATH` is set → Private key authentication
2. Otherwise, if `SFTP_PASSWORD` is set → Password authentication
3. If neither is set → Connection fails

**Security Notes:**
- ✅ `.env` is already in `.gitignore` (not committed to git)
- ✅ **Private key authentication is more secure than passwords**
- ✅ Use read-only SFTP account if possible
- ✅ Set restrictive permissions: `chmod 600 .env`
- ✅ Protect your private key: `chmod 600 ~/.ssh/id_rsa`

## Usage

### Command Line Options

```bash
uv run run_local_with_sftp.py [OPTIONS]
```

**Options:**

| Option | Choices | Default | Description |
|--------|---------|---------|-------------|
| `--env` | local | local | Execution environment (only local supported) |
| `--profile` | Staging, CertDC, Helios, ... | Staging | DBT profile to execute |
| `--use-sftp` | flag | False | Download files from SFTP before running |

### Examples

**Staging pipeline with manual files:**
```bash
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

**Staging pipeline with SFTP download:**
```bash
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

**Helios pipeline with manual files:**
```bash
uv run run_local_with_sftp.py --env "local" --profile "Helios"
```

## Pipeline Execution Flow

### Without SFTP (`--use-sftp` not provided)

```
📂 STEP 1: Using manual CSV files
   ├─ Check input/staging/ for CSV files
   └─ Report: "Found X CSV files"

🦆 STEP 2: Initialize DuckDB connection
   └─ Connect to data/staging/duckdb_database.duckdb

📊 STEP 3: Load CSV data into DuckDB
   ├─ Read CSV files from input/staging/
   ├─ Create tables from output_sql/staging/*.sql
   ├─ Standardize column names (lowercase, no accents)
   ├─ Handle duplicates (commentaire → commentaire_1, commentaire_2)
   ├─ Truncate long names (63 char limit)
   └─ Load data into tables

🔄 STEP 4: Run DBT transformations
   ├─ Execute dbt run (create views)
   ├─ Execute dbt test (validate data)
   └─ ✅ Pipeline completed successfully!
```

### With SFTP (`--use-sftp` provided)

```
📥 STEP 1: Download files from SFTP
   ├─ Connect to SFTP server (credentials from .env)
   ├─ For each file in metadata.yml files_to_download:
   │   ├─ Search for latest file matching keyword
   │   ├─ Download: sirec_20251026.csv
   │   └─ Save as: sa_sirec.csv (✅ already renamed!)
   └─ ✅ SFTP download complete

🦆 STEP 2: Initialize DuckDB connection
   └─ Connect to data/staging/duckdb_database.duckdb

📊 STEP 3: Load CSV data into DuckDB
   [same as without SFTP]

🔄 STEP 4: Run DBT transformations
   [same as without SFTP]
```

## File Naming

### Key Insight: SFTP Handles Renaming Automatically! ✅

When using `--use-sftp`, files are downloaded **and renamed** in one step based on `metadata.yml`:

```yaml
# metadata.yml
files_to_download:
  - path: "/SCN_BDD/SIREC"
    keyword: "sirec_20250801"    # Search pattern
    file: "sa_sirec.csv"         # ✅ Target name (already correct!)
```

**Result:**
- Remote file: `sirec_20251026.csv`
- Downloaded as: `sa_sirec.csv`
- ✅ No separate rename step needed!

### File Naming Comparison

| Method | Source File | Renamed By | Final Name | Needs rename_staging_files.py? |
|--------|-------------|------------|------------|-------------------------------|
| Manual copy | `sirec_20251026.csv` | `rename_staging_files.py` | `sa_sirec.csv` | ✅ Yes |
| SFTP download | `sirec_20251026.csv` | `SFTPSync.download_all()` | `sa_sirec.csv` | ❌ No (automatic!) |

## Expected Files

When the pipeline runs, it expects these CSV files in `input/staging/`:

| File Name | Source | Description |
|-----------|--------|-------------|
| `sa_sirec.csv` | SIREC | Reportable events |
| `sa_sivss.csv` | SIVSS | Social security inspection visits |
| `sa_siicea_decisions.csv` | SIICEA | Inspection decisions |
| `sa_siicea_cibles.csv` | SIICEA | Inspection targets |
| `sa_siicea_missions_prog.csv` | SIICEA | Planned missions |
| `sa_siicea_missions_real.csv` | SIICEA | Completed missions |
| `sa_t_finess.csv` | FINESS | Healthcare facilities |
| `sa_insern.csv` | INSERN | Death certificates |
| ... (and 17 more) | | See metadata.yml for full list |

## Troubleshooting

### SFTP Connection Errors

**Error:** `SFTP download failed: Authentication failed`

**Solution:**
1. Check `.env` file exists in `DBT/anais_staging/`
2. Verify credentials are correct
3. Test SFTP connection manually: `sftp username@host`

### Missing CSV Files

**Error:** `Cannot populate DuckDB database - Empty directories`

**Solution:**
1. **If using manual files:** Copy files to `input/staging/` and run `rename_staging_files.py`
2. **If using SFTP:** Check SFTP credentials and `metadata.yml` configuration

### Database Empty After Loading

**Error:** `Database duckdb_database.duckdb is empty`

**Solution:**
1. Check CSV files have correct format (not empty, valid delimiters)
2. Check SQL schemas exist in `output_sql/staging/`
3. Review logs in `logs/log_local_sftp.log` for detailed errors

### Schema Mismatch Errors

**Error:** Column count mismatch between CSV and SQL schema

**Solution:**
1. Run validation script: `python3 ../../validate_csv_schemas.py`
2. Check if CSV delimiter is correct (;, ,, or ¤)
3. Update SQL schema if needed

## Logs

Execution logs are written to: `logs/log_local_sftp.log`

**Log format:**
```
2024-10-26 14:30:15 - INFO - ================================================================================
2024-10-26 14:30:15 - INFO - 🚀 LOCAL STAGING PIPELINE WITH SFTP SUPPORT
2024-10-26 14:30:15 - INFO - ================================================================================
2024-10-26 14:30:15 - INFO - Environment: local
2024-10-26 14:30:15 - INFO - Profile: Staging
2024-10-26 14:30:15 - INFO - SFTP Download: ✅ Enabled
2024-10-26 14:30:15 - INFO - Database: data/staging/duckdb_database.duckdb
```

## Comparison with Standard Pipeline

| Feature | `uv run -m pipeline.main` | `uv run run_local_with_sftp.py` |
|---------|--------------------------|----------------------------------|
| SFTP download (local) | ❌ Not available | ✅ Available with `--use-sftp` |
| DuckDB loading | ✅ Yes | ✅ Yes |
| DBT transformations | ✅ Yes | ✅ Yes |
| Custom logging | ❌ Generic log | ✅ Detailed step-by-step log |
| Modifiable | ❌ Installed package | ✅ Local script (can customize) |

## Integration with Ingestion Pipeline

You can combine this script with the ingestion pipeline for a complete workflow:

```bash
# Option 1: Ingestion pipeline + DBT (manual files)
cd ingestion
python3 ingestion.py --execute  # Download, validate, rename
cd ../DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging"

# Option 2: SFTP download directly (bypasses ingestion)
cd DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

## FAQ

**Q: Do I need .env file for local execution without SFTP?**
A: No! Only needed if using `--use-sftp` flag.

**Q: Can I use this for Helios or other profiles?**
A: Yes, just change `--profile` to desired profile (Helios, CertDC, etc.)

**Q: Does this replace the standard pipeline?**
A: No, it's an alternative. Both work:
- Standard: `uv run -m pipeline.main --env "local" --profile "Staging"`
- SFTP-enabled: `uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp`

**Q: How do I update to latest pipeline code?**
A: The script uses the installed `pipeline` package. To update:
```bash
rm uv.lock
uv sync
```

**Q: What if I want to download only some files from SFTP?**
A: Edit `metadata.yml` and comment out unwanted files:
```yaml
files_to_download:
  - path: "/SCN_BDD/SIREC"
    keyword: "sirec"
    file: "sa_sirec.csv"
  # - path: "/SCN_BDD/SIVSS"  # Commented - won't download
  #   keyword: "SIVSS"
  #   file: "sa_sivss.csv"
```

## Next Steps

1. ✅ **Start simple:** Try without SFTP first using manual files
2. ✅ **Validate data:** Use `validate_csv_schemas.py` to check file formats
3. ✅ **Enable SFTP:** Add .env file and try `--use-sftp` flag
4. ✅ **Automate:** Combine with ingestion pipeline or create bash scripts

## Support

For issues or questions:
- Check logs in `logs/log_local_sftp.log`
- Review `metadata.yml` configuration
- Verify CSV file formats with `validate_csv_schemas.py`
- Consult `LOCAL_PIPELINE_GUIDE.md` for detailed pipeline documentation
