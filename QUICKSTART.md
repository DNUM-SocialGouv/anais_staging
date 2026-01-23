# ANAIS Staging Pipeline - Quick Start Guide

## Two Ways to Run the Pipeline

### Option 1: With SFTP Download (Recommended)

Downloads the latest files automatically from SFTP server:

```bash
cd DBT/anais_staging

# 1. Setup SFTP credentials (one-time)
cat > .env << 'EOF'
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
# Optional: SFTP_PRIVATE_KEY_PASSPHRASE="your_passphrase"
# Alternative: SFTP_PASSWORD="your_password"
EOF
chmod 600 .env

# 2. Install dependencies
uv sync

# 3. Run pipeline with SFTP
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

**What it does:**
- ✅ Downloads 25 CSV files from SFTP
- ✅ Renames files automatically (e.g., `sirec_20251026.csv` → `sa_sirec.csv`)
- ✅ Tracks input file dates in `input_files_date` table
- ✅ Applies pipeline patches (boolean conversion, historisation safety)
- ✅ Loads data into DuckDB (`data/staging/duckdb_database.duckdb`)
- ✅ Runs DBT transformations

**Files downloaded:** Configured in `metadata.yml` under `files_to_download`

### Option 2: With Manual Files

Use CSV files you've copied manually:

```bash
cd DBT/anais_staging

# 1. Copy and rename CSV files to input/staging/
cp /path/to/sirec_*.csv input/staging/sa_sirec.csv
cp /path/to/SIVSS_*.csv input/staging/sa_sivss.csv
cp /path/to/SIICEA_DECISIONS_*.csv input/staging/sa_siicea_decisions.csv
# ... copy all needed files (see metadata.yml for expected filenames)

# 2. Install dependencies
uv sync

# 3. Run pipeline (without --use-sftp)
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

**What it does:**
- ✅ Uses files from `input/staging/`
- ✅ Applies pipeline patches (boolean conversion, historisation safety)
- ✅ Loads data into DuckDB
- ✅ Runs DBT transformations

**Note:** Without SFTP, date tracking uses the local filename as-is (cannot extract date from original SFTP filename).

## Validate CSV Files

Before running the pipeline, validate your CSV files:

```bash
cd DBT/anais_staging
python3 tests/validate_csv_schemas.py
```

This checks:
- ✅ Correct number of columns
- ✅ Column names match expected schema
- ✅ Proper delimiters (`;`, `,`, `¤`)

## Expected Files

The pipeline expects these files in `input/staging/` (configured in `metadata.yml`):

| File | Source | Category |
|------|--------|----------|
| `sa_sirec.csv` | SIREC | Core |
| `sa_sivss.csv` | SIVSS | Core |
| `sa_siicea_decisions.csv` | SIICEA | Core |
| `sa_siicea_cibles.csv` | SIICEA | Core |
| `sa_siicea_missions_prog.csv` | SIICEA | Core |
| `sa_siicea_missions_real.csv` | SIICEA | Core |
| `sa_t_finess.csv` | T_FINESS | Reference |
| `sa_insern.csv` | INSERN | Health |
| `v_commune.csv`, `v_departement.csv`, `v_region.csv` | INSEE | Geographic |
| `v_comer.csv`, `v_commune_comer.csv`, `v_commune_depuis.csv` | INSEE | Geographic |
| `cert_dc_insern_n2_n1.csv`, `cert_dc_insern_2023_2024.csv` | INSERN | CertDC |
| `sa_esms.csv`, `sa_pmsi.csv`, `sa_rpu.csv`, `sa_usld.csv` | DIAMANT | Health |
| `sa_hubee.csv` | HUBEE | Other |
| `dc_det.csv`, `sa_insee_histo.csv` | INSEE | Other |
| `sa_ciblage.csv`, `sa_tdb_esms.csv` | TMP | Temporary |

**With SFTP:** All 25 files downloaded automatically
**Without SFTP:** Copy needed files manually

## Output

After successful execution:

```
data/staging/
└── duckdb_database.duckdb      # DuckDB database with tables and views

dbtStaging/target/               # DBT artifacts

logs/
└── log_local_sftp.log          # Execution logs
```

### File Date Tracking

The pipeline automatically tracks input file dates in the `input_files_date` table:

```sql
-- Query tracked file dates
SELECT * FROM input_files_date;
```

| Column | Description |
|--------|-------------|
| `source_system` | Source system (SIVSS, SIREC, SIICEA) |
| `file_type` | File type identifier |
| `original_filename` | Original filename from SFTP (e.g., `SIVSS_SCN_20251007.csv`) |
| `local_filename` | Local filename (e.g., `sa_sivss.csv`) |
| `extracted_date` | Date extracted from original filename |
| `ingestion_timestamp` | When the file was ingested |

This table is used by the Helios pipeline to generate output filenames with the correct dates.

Query the database:

```bash
duckdb data/staging/duckdb_database.duckdb
```

```sql
SHOW TABLES;
SELECT COUNT(*) FROM sa_sirec;
SELECT * FROM staging__sa_sirec LIMIT 10;
```

## Troubleshooting

### SFTP Connection Issues

```bash
# Test SSH connection manually
ssh -i ~/.ssh/id_rsa username@sftp.host

# Check key permissions
chmod 600 ~/.ssh/id_rsa
chmod 600 .env

# View detailed logs
cat logs/log_local_sftp.log | grep SFTP
```

### CSV Validation Errors

```bash
# Run validation
python3 tests/validate_csv_schemas.py

# Check delimiter
head -1 input/staging/sa_sirec.csv | tr ';' '\n' | wc -l

# Check encoding
file input/staging/sa_sirec.csv
```

### Empty Database

```bash
# Check files exist
ls -l input/staging/*.csv

# Check SQL schemas exist
ls -l output_sql/staging/*.sql

# Review logs
tail -100 logs/log_local_sftp.log
```

## Documentation

- **Full Documentation:** [README.md](README.md)
- **SFTP Configuration:** See `metadata.yml` for file download mappings
- **CSV Validation Tests:** [tests/README.md](tests/README.md)

## Commands Reference

```bash
# With SFTP
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp

# Without SFTP
uv run run_local_with_sftp.py --env "local" --profile "Staging"

# Validate CSV files
python3 tests/validate_csv_schemas.py

# Query database
duckdb data/staging/duckdb_database.duckdb
```

## What's Next?

After staging pipeline completes, run the Helios pipeline:

```bash
cd ../anais_helios
uv sync
uv run run_local_with_sftp.py --env "local" --profile "Helios"
```

Helios will:
- Copy tables from Staging database
- Run analytical transformations
- Export CSV files to `output/helios/` (with dates from input files)

**With SFTP upload:**
```bash
uv run run_local_with_sftp.py --env "local" --profile "Helios" --use-sftp
```
