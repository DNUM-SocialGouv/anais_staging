# Boolean Column Fix - Development Plan

## Problem Statement

**Issue:** SIVSS CSV files contain boolean columns with lowercase string values (`"true"` and `"false"`), but when loaded into DuckDB, all values are converted to `True`.

**Root Cause:** In `pipeline/utils/csv_management.py` line 370, the code uses:
```python
self.df[col_name] = self.df[col_name].replace({None: False, "": False, pd.NA: False}).astype(bool)
```

When pandas converts strings to boolean with `.astype(bool)`, **any non-empty string evaluates to `True`**, including the string `"false"`.

**Expected Behavior:**
- `"true"` → `True` (boolean)
- `"false"` → `False` (boolean)

**Actual Behavior:**
- `"true"` → `True` ✅
- `"false"` → `True` ❌ (incorrect!)

## Affected Columns

In `sa_sivss.csv`, the following BOOLEAN columns are affected:

1. `est_eigs` (column 10)
2. `reclamation` (column 12)
3. `declarant_est_anonyme` (column 13)
4. `survenue_cas_collectivite` (column 25)

## Analysis

### Current Code Logic
```python
# Line 370 in csv_management.py
self.df[col_name] = self.df[col_name].replace({None: False, "": False, pd.NA: False}).astype(bool)
```

**Problem:**
- `.astype(bool)` converts string `"false"` to boolean `True` because it's a non-empty string
- This is standard Python behavior: `bool("false")` returns `True`

### Why This Happens

```python
# Python boolean conversion examples
bool("true")   # True  ✅
bool("false")  # True  ❌ - any non-empty string!
bool("0")      # True  ❌
bool("")       # False
bool(None)     # False
```

## Solution Approach

### Option 1: String Mapping (RECOMMENDED)

Map string values explicitly before converting to boolean:

```python
# Map string representations to actual booleans
bool_map = {
    'true': True,
    'True': True,
    'TRUE': True,
    '1': True,
    'false': False,
    'False': False,
    'FALSE': False,
    '0': False,
    None: False,
    '': False,
    pd.NA: False,
    'nan': False,
    'NaN': False
}
self.df[col_name] = self.df[col_name].map(bool_map).fillna(False).astype(bool)
```

**Pros:**
- ✅ Handles all string variations (lowercase, uppercase, mixed case)
- ✅ Explicitly defines mapping
- ✅ Handles None, NA, and empty strings
- ✅ Clear and maintainable

**Cons:**
- ⚠️ Need to handle unmapped values

### Option 2: Case-Insensitive String Comparison

```python
# Convert to lowercase and compare
self.df[col_name] = self.df[col_name].fillna('false').astype(str).str.lower() == 'true'
```

**Pros:**
- ✅ Simple one-liner
- ✅ Case-insensitive

**Cons:**
- ⚠️ Assumes only 'true'/'false' values
- ⚠️ Non-boolean strings default to False (might hide data issues)

### Option 3: Custom Boolean Parser Function

```python
def parse_boolean(value):
    if pd.isna(value) or value == '' or value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    # String handling
    str_val = str(value).strip().lower()
    if str_val in ('true', '1', 'yes', 'y', 't'):
        return True
    elif str_val in ('false', '0', 'no', 'n', 'f'):
        return False
    else:
        # Log warning for unexpected values
        return False

self.df[col_name] = self.df[col_name].apply(parse_boolean)
```

**Pros:**
- ✅ Most robust
- ✅ Handles multiple formats
- ✅ Can log warnings for unexpected values

**Cons:**
- ⚠️ More complex
- ⚠️ Slower (uses .apply())

## Recommended Solution

**Use Option 1 (String Mapping)** with the following implementation:

```python
elif self.type_mapping[col_type] == "bool":
    # Create mapping for string boolean values
    bool_map = {
        'true': True, 'True': True, 'TRUE': True,
        'false': False, 'False': False, 'FALSE': False,
        '1': True, 1: True,
        '0': False, 0: False,
        None: False, '': False, pd.NA: False,
        'nan': False, 'NaN': False, 'NAN': False
    }

    # Map values and handle unmapped cases
    self.df[col_name] = self.df[col_name].map(bool_map)

    # Fill any remaining NaN values (unmapped) with False
    self.df[col_name] = self.df[col_name].fillna(False).astype(bool)
```

**Rationale:**
- Clear and explicit mapping
- Handles all common boolean representations
- Performant (uses vectorized operations)
- Safe fallback for unmapped values

## Implementation Steps

### Step 1: Locate the File
The bug is in the **installed package**, not in the local repository:
```
.venv/lib/python3.12/site-packages/pipeline/utils/csv_management.py
```

### Step 2: Create a Fix

Since we cannot modify the installed package directly, we have **two options**:

#### Option A: Create a Local Override (QUICK FIX)
Create a patched version that the pipeline uses before loading

#### Option B: Fix in Source Repository (PROPER FIX)
- Fork `anais_pipeline` repository
- Fix the bug in source code
- Update `pyproject.toml` to use the fixed version

### Step 3: Test the Fix

```bash
# 1. Verify current behavior (before fix)
duckdb data/staging/duckdb_database.duckdb -c "SELECT est_eigs, reclamation, declarant_est_anonyme FROM sa_sivss LIMIT 20;"

# 2. Apply fix

# 3. Re-run pipeline
uv run run_local_with_sftp.py --env "local" --profile "Staging"

# 4. Verify fixed behavior (after fix)
duckdb data/staging/duckdb_database.duckdb -c "SELECT est_eigs, reclamation, declarant_est_anonyme, COUNT(*) FROM sa_sivss GROUP BY 1,2,3;"
```

### Step 4: Validate Results

Expected distribution in SIVSS data:
```sql
-- Should see mix of True and False values
SELECT
    est_eigs,
    COUNT(*) as count
FROM sa_sivss
GROUP BY est_eigs;

-- Should return:
-- est_eigs | count
-- ---------|-------
-- false    | ~XXXX
-- true     | ~XXXX
```

## Testing Strategy

### Unit Test Cases

```python
# Test cases for boolean conversion
test_cases = [
    ("true", True),
    ("false", False),
    ("True", True),
    ("False", False),
    ("TRUE", True),
    ("FALSE", False),
    ("1", True),
    ("0", False),
    (1, True),
    (0, False),
    (None, False),
    ("", False),
    (pd.NA, False),
    ("nan", False),
]
```

### Integration Test

```bash
# Create test CSV with boolean values
cat > test_boolean.csv << 'EOF'
col1,col2,col3
true,false,True
false,true,False
1,0,true
EOF

# Test loading
# Expected: col1=[True,False,True], col2=[False,True,False], col3=[True,False,True]
```

## Risks and Considerations

### Risks
1. **Breaking change**: Other CSV files might use different boolean formats
2. **Unmapped values**: Need to handle values not in the mapping
3. **Performance**: Mapping operation on large datasets

### Mitigations
1. **Comprehensive mapping**: Include all common formats
2. **Safe fallback**: Use `.fillna(False)` for unmapped values
3. **Logging**: Add warning logs for unmapped values (optional)
4. **Testing**: Test with actual SIVSS data

## Rollback Plan

If the fix causes issues:

1. **Revert to previous version**:
   ```bash
   cd DBT/anais_staging
   rm uv.lock
   # Edit pyproject.toml to pin previous pipeline version
   uv sync
   ```

2. **Use manual override**:
   - Modify CSV files before loading
   - Convert "false" to "0" and "true" to "1"

## Documentation Updates

After fix is implemented:

1. Update `QUICKSTART.md` - Note about boolean handling
2. Create `CHANGELOG.md` entry
3. Document in pipeline repository README
4. Add to troubleshooting guide

## Timeline

- **Analysis**: ✅ Complete
- **Plan Creation**: ✅ Complete
- **Implementation**: 30 minutes
- **Testing**: 15 minutes
- **Documentation**: 15 minutes

**Total estimated time**: ~1 hour

## Success Criteria

✅ Boolean columns in SIVSS correctly show `True` and `False` values
✅ No data loss or corruption
✅ Other CSV files (SIREC, SIICEA) still load correctly
✅ Pipeline completes without errors
✅ DBT tests pass
