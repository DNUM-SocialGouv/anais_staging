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
- ✅ Loads data into DuckDB (`data/staging/duckdb_database.duckdb`)
- ✅ Runs DBT transformations

**Files downloaded:** See [SFTP_FILENAME_MAPPING.md](SFTP_FILENAME_MAPPING.md) for complete list

### Option 2: With Manual Files

Use CSV files you've copied manually:

```bash
cd DBT/anais_staging

# 1. Copy and rename CSV files
cp /path/to/sirec_*.csv input/staging/sa_sirec.csv
cp /path/to/SIVSS_*.csv input/staging/sa_sivss.csv
cp /path/to/SIICEA_DECISIONS_*.csv input/staging/sa_siicea_decisions.csv
# ... copy all needed files

# 2. Install dependencies
uv sync

# 3. Run pipeline
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

**What it does:**
- ✅ Uses files from `input/staging/`
- ✅ Loads data into DuckDB
- ✅ Runs DBT transformations

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

The pipeline expects these files in `input/staging/`:

| File | Source | Required |
|------|--------|----------|
| `sa_sirec.csv` | SIREC | Core |
| `sa_sivss.csv` | SIVSS | Core |
| `sa_siicea_decisions.csv` | SIICEA | Core |
| `sa_siicea_cibles.csv` | SIICEA | Core |
| `sa_siicea_missions_prog.csv` | SIICEA | Core |
| `sa_siicea_missions_real.csv` | SIICEA | Core |
| `sa_t_finess.csv` | FINESS | Reference |
| `sa_insern.csv` | INSERN | Health |
| ... (19 more files) | Various | Optional |

**With SFTP:** All files downloaded automatically
**Without SFTP:** Copy at least the 6 core files

## Output

After successful execution:

```
data/staging/
└── duckdb_database.duckdb      # DuckDB database with tables and views

dbtStaging/target/               # DBT artifacts

logs/
└── log_local_sftp.log          # Execution logs
```

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

- **SFTP Setup:** [PRIVATE_KEY_SETUP_GUIDE.md](PRIVATE_KEY_SETUP_GUIDE.md)
- **SFTP Files:** [SFTP_FILENAME_MAPPING.md](SFTP_FILENAME_MAPPING.md)
- **SFTP Usage:** [SFTP_USAGE_GUIDE.md](SFTP_USAGE_GUIDE.md)
- **Original README:** [README.md](README.md)

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
uv run -m pipeline.main --env "local" --profile "Helios"
```

Helios will:
- Copy tables from Staging database
- Run analytical transformations
- Export CSV files to `output/helios/`
