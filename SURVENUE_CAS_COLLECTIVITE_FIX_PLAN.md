# Fix Plan: survenue_cas_collectivite Column Boolean Issue

**Date:** October 27, 2024
**Issue:** The `survenue_cas_collectivite` column is still showing as BOOLEAN in existing databases
**Status:** ✅ SQL Schema Already Fixed, Database Rebuild Required

## Problem Analysis

### Current Situation

The user reports that `survenue_cas_collectivite` (which may appear as `scc_cas_collectivite` in some contexts) is still showing as BOOLEAN type, even though:

1. ✅ The SQL schema files have been updated to VARCHAR(5)
2. ✅ All 4 boolean columns in SIVSS were changed in the same commit:
   - `est_eigs` → VARCHAR(5)
   - `reclamation` → VARCHAR(5)
   - `declarant_est_anonyme` → VARCHAR(5)
   - `survenue_cas_collectivite` → VARCHAR(5)

### Root Cause

**Existing databases were created with the old BOOLEAN schema** and need to be rebuilt with the new VARCHAR(5) schema.

When DuckDB creates a table from a SQL schema file, it uses the schema definition at creation time. If the database was created before the VARCHAR(5) fix, the table still has BOOLEAN columns.

### Evidence

**CSV Input (Column 25 - SURVENUE_CAS_COLLECTIVITE):**
```bash
$ head -100 input/staging/sa_sivss.csv | cut -d'¤' -f25 | sort | uniq -c
73 false
26 true
```

**Current SQL Schemas:**
```sql
-- DBT/anais_staging/output_sql/staging/sa_sivss.sql (line 26)
survenue_cas_collectivite VARCHAR(5),

-- DBT/anais_helios/output_sql/helios/sa_sivss.sql (line 26)
survenue_cas_collectivite VARCHAR(5),
```

**Existing Database (if not rebuilt):**
```sql
-- Old schema still in database
survenue_cas_collectivite BOOLEAN
```

## Solution

### Approach: Delete and Rebuild Databases

Since the SQL schemas are already correct, we just need to:
1. Delete existing databases (they have old BOOLEAN schema)
2. Run pipelines to create new databases (with new VARCHAR(5) schema)
3. Verify the column types are now VARCHAR(5)

## Files Already Fixed

✅ All SQL schema files are correct:

**Staging:**
- `DBT/anais_staging/output_sql/staging/sa_sivss.sql` - Line 26: `survenue_cas_collectivite VARCHAR(5)`

**Helios:**
- `DBT/anais_helios/output_sql/helios/sa_sivss.sql` - Line 26: `survenue_cas_collectivite VARCHAR(5)`

## Implementation Steps

### Step 1: Verify SQL Schemas (Already Done)

Confirm all 4 columns are VARCHAR(5) in both Staging and Helios schemas:

```bash
# Check Staging schema
grep -n "est_eigs\|reclamation\|declarant_est_anonyme\|survenue_cas_collectivite" \
  DBT/anais_staging/output_sql/staging/sa_sivss.sql

# Expected output:
# 11:    est_eigs VARCHAR(5),
# 13:    reclamation VARCHAR(5),
# 14:    declarant_est_anonyme VARCHAR(5),
# 26:    survenue_cas_collectivite VARCHAR(5),

# Check Helios schema
grep -n "est_eigs\|reclamation\|declarant_est_anonyme\|survenue_cas_collectivite" \
  DBT/anais_helios/output_sql/helios/sa_sivss.sql

# Expected output:
# 11:    est_eigs VARCHAR(5),
# 13:    reclamation VARCHAR(5),
# 14:    declarant_est_anonyme VARCHAR(5),
# 26:    survenue_cas_collectivite VARCHAR(5),
```

✅ **Status:** VERIFIED - All schemas are correct

### Step 2: Delete Existing Databases

Remove old databases that were created with BOOLEAN schema:

```bash
# Delete Staging database
cd DBT/anais_staging
rm -f data/staging/duckdb_database.duckdb

# Delete Helios database
cd ../anais_helios
rm -f data/helios/duckdb_database.duckdb
```

### Step 3: Rebuild Staging Database

Run the Staging pipeline to create a fresh database with VARCHAR(5) columns:

```bash
cd DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

**What this does:**
1. Creates new DuckDB database
2. Uses `output_sql/staging/sa_sivss.sql` to define table structure
3. Loads CSV data with VARCHAR(5) columns
4. Values remain as `"true"` and `"false"` strings

### Step 4: Rebuild Helios Database

Run the Helios pipeline to create a fresh database:

```bash
cd ../anais_helios
uv run run_local_with_sftp.py --env "local" --profile "Helios"
```

**What this does:**
1. Creates new DuckDB database
2. Uses `output_sql/helios/sa_sivss.sql` to define table structure
3. Copies data from Staging (which now has VARCHAR(5) columns)
4. Runs DBT transformations
5. Exports CSV files with `"true"`/`"false"` string values

### Step 5: Verify Column Types

Check that the databases now have VARCHAR columns:

```python
import duckdb

# Check Staging database
conn = duckdb.connect('DBT/anais_staging/data/staging/duckdb_database.duckdb')
schema = conn.execute("DESCRIBE sa_sivss").fetchall()
boolean_cols = [col for col in schema if col[0] in ['est_eigs', 'reclamation', 'declarant_est_anonyme', 'survenue_cas_collectivite']]
print("Staging schema:")
for col in boolean_cols:
    print(f"  {col[0]}: {col[1]}")
# Expected: All should show VARCHAR

# Check Helios database
conn = duckdb.connect('DBT/anais_helios/data/helios/duckdb_database.duckdb')
schema = conn.execute("DESCRIBE sa_sivss").fetchall()
boolean_cols = [col for col in schema if col[0] in ['est_eigs', 'reclamation', 'declarant_est_anonyme', 'survenue_cas_collectivite']]
print("\nHelios schema:")
for col in boolean_cols:
    print(f"  {col[0]}: {col[1]}")
# Expected: All should show VARCHAR
```

### Step 6: Verify Data Values

Check that the values are lowercase strings:

```python
import duckdb

# Check Staging data
conn = duckdb.connect('DBT/anais_staging/data/staging/duckdb_database.duckdb')
result = conn.execute("""
    SELECT survenue_cas_collectivite, COUNT(*)
    FROM sa_sivss
    GROUP BY survenue_cas_collectivite
""").fetchall()
print("Staging data:")
print(result)
# Expected: [("false", ~count1), ("true", ~count2)]  # Strings!

# Check Helios data
conn = duckdb.connect('DBT/anais_helios/data/helios/duckdb_database.duckdb')
result = conn.execute("""
    SELECT survenue_cas_collectivite, COUNT(*)
    FROM sa_sivss
    GROUP BY survenue_cas_collectivite
""").fetchall()
print("\nHelios data:")
print(result)
# Expected: [("false", ~count1), ("true", ~count2)]  # Strings!
```

### Step 7: Verify Output CSV Files

Check that the Helios output CSV files contain lowercase string values:

```bash
cd DBT/anais_helios

# Check for "true" and "false" in output files
grep -o -E "(^|¤)(true|false)(¤|$)" output/helios/test_helios_sivss_*.csv | head -20

# Should show lowercase "true" and "false", not "True" and "False"
```

## Why This Happens

### Database Schema is Created at Table Creation Time

When DuckDB executes:
```sql
CREATE TABLE IF NOT EXISTS sa_sivss (
    survenue_cas_collectivite BOOLEAN,
    ...
)
```

The table structure is permanently set to BOOLEAN. Even if we later change the SQL file to:
```sql
CREATE TABLE IF NOT EXISTS sa_sivss (
    survenue_cas_collectivite VARCHAR(5),
    ...
)
```

The `IF NOT EXISTS` clause means the table is **not recreated** if it already exists. The old BOOLEAN schema persists.

### Solution: Force Recreation

Deleting the database file forces recreation with the new schema.

## Expected Results

### Before Fix (Old Database)

```python
# Schema
survenue_cas_collectivite: BOOLEAN

# Data
[('True', 269929)]  # All values converted to True
```

### After Fix (New Database)

```python
# Schema
survenue_cas_collectivite: VARCHAR

# Data
[('false', ~683000), ('true', ~126000)]  # Lowercase strings preserved
```

## Testing Checklist

- [ ] Delete Staging database
- [ ] Delete Helios database
- [ ] Run Staging pipeline
- [ ] Run Helios pipeline
- [ ] Verify Staging schema (VARCHAR)
- [ ] Verify Helios schema (VARCHAR)
- [ ] Verify Staging data (lowercase strings)
- [ ] Verify Helios data (lowercase strings)
- [ ] Verify output CSV files (lowercase strings)

## Rollback

If the fix causes issues:

1. **Revert SQL schemas to BOOLEAN:**
   ```bash
   # In both files, change VARCHAR(5) back to BOOLEAN
   # DBT/anais_staging/output_sql/staging/sa_sivss.sql
   # DBT/anais_helios/output_sql/helios/sa_sivss.sql
   ```

2. **Delete databases and rebuild:**
   ```bash
   rm DBT/anais_staging/data/staging/duckdb_database.duckdb
   rm DBT/anais_helios/data/helios/duckdb_database.duckdb
   ```

## Summary

**Issue:** `survenue_cas_collectivite` appears as BOOLEAN in existing databases

**Root Cause:** Databases created with old schema before VARCHAR(5) fix

**Solution:** Delete and rebuild databases (SQL schemas already fixed)

**Impact:** All 4 boolean columns will preserve lowercase `"true"`/`"false"` strings

**Duration:** ~5-10 minutes (pipeline rebuild time)

**Risk:** Low (just database rebuild, no code changes needed)
