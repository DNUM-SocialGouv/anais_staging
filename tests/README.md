# CSV Validation Tests

## validate_csv_schemas.py

Validates CSV files in `input/staging/` against expected schemas.

### Usage

```bash
cd DBT/anais_staging
python3 tests/validate_csv_schemas.py
```

### What It Checks

- ✅ Column count matches expected schema
- ✅ Column names match (normalized)
- ✅ Correct delimiters (`;`, `,`, `¤`)
- ✅ No missing or extra columns

### Expected Files

- `sa_sirec.csv` (69 columns, `;` delimiter)
- `sa_sivss.csv` (39 columns, `¤` delimiter)
- `sa_siicea_decisions.csv` (32 columns, `,` delimiter)
- `sa_siicea_cibles.csv` (14 columns, `,` delimiter)
- `sa_siicea_missions_prog.csv` (96 columns, `,` delimiter)
- `sa_siicea_missions_real.csv` (96 columns, `,` delimiter)

### Output

```
================================================================================
CSV Schema Validation - input/staging/ Directory
================================================================================

Found 6 CSV file(s) to validate

sa_sirec.csv (sa_sirec)
  Status: ✓ VALID
  Delimiter: ;
  Columns: 68 (expected: 68)

================================================================================
Summary
================================================================================
Total files: 6
Valid: 6
```

### Exit Codes

- `0`: All files valid
- `1`: At least one file invalid or error occurred

### Integration

Run this before the pipeline to ensure data quality:

```bash
# Validate then run pipeline
python3 tests/validate_csv_schemas.py && \
  uv run run_local_with_sftp.py --env "local" --profile "Staging"
```
