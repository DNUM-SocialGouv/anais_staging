# SFTP Filename Mapping Reference

## Overview

When using `--use-sftp`, the pipeline downloads files from SFTP and **automatically renames them** based on the configuration in `metadata.yml`. This document shows the exact mapping for all 25 expected files.

## How SFTP Renaming Works

The SFTP download process:

1. **Searches** for the latest file in the specified path containing the keyword
2. **Downloads** that file
3. **Renames** it to the target filename during download
4. **Saves** it to `input/staging/`

**Example:**
- Remote file: `sirec_20251026_142530.csv`
- Keyword match: `"sirec_20250801"`
- Downloaded as: `sa_sirec.csv`
- Location: `input/staging/sa_sirec.csv`

## Complete Filename Mapping Table

| # | SFTP Path | Search Keyword | Example Remote Filename | Downloaded As | Description |
|---|-----------|----------------|------------------------|---------------|-------------|
| 1 | `/SCN_BDD/INSERN` | `DNUM_TdB_CertDc` | `DNUM_TdB_CertDc_20251026.csv` | `sa_insern.csv` | Death certificates (INSERN) |
| 2 | `/SCN_BDD/SIICEA` | `GROUPECIBLES_SCN` | `SIICEA_GROUPECIBLES_SCN_202510200320_Production.csv` | `sa_siicea_cibles.csv` | Inspection targets |
| 3 | `/SCN_BDD/SIICEA` | `DECISIONS_SCN` | `SIICEA_DECISIONS_SCN_202510200320_Production.csv` | `sa_siicea_decisions.csv` | Inspection decisions |
| 4 | `/SCN_BDD/SIREC` | `sirec_20250801` | `sirec_20251026.csv` | `sa_sirec.csv` | Reportable events |
| 5 | `/SCN_BDD/SIVSS` | `SIVSS_SCN` | `SIVSS_SCN_202510070200_prod.csv` | `sa_sivss.csv` | Social security visits |
| 6 | `/SCN_BDD/T_FINESS` | `t-finess` | `t-finess_20251026.csv` | `sa_t_finess.csv` | Healthcare facilities |
| 7 | `/SCN_BDD/INSEE` | `v_comer` | `v_comer_2024.csv` | `v_comer.csv` | INSEE COMER reference |
| 8 | `/SCN_BDD/INSEE` | `v_commune_comer` | `v_commune_comer_2024.csv` | `v_commune_comer.csv` | Municipality COMER |
| 9 | `/SCN_BDD/INSEE` | `v_commune_depuis` | `v_commune_depuis_2024.csv` | `v_commune_depuis.csv` | Municipality history |
| 10 | `/SCN_BDD/INSEE` | `v_commune_2024` | `v_commune_2024.csv` | `v_commune.csv` | Municipalities 2024 |
| 11 | `/SCN_BDD/INSEE` | `v_departement` | `v_departement_2024.csv` | `v_departement.csv` | Departments |
| 12 | `/SCN_BDD/INSEE` | `v_region` | `v_region_2024.csv` | `v_region.csv` | Regions |
| 13 | `/SCN_BDD/INSERN/historique` | `sa_insern_n_2_n_1` | `sa_insern_n_2_n_1_20241026.csv` | `cert_dc_insern_n2_n1.csv` | Death certs historical (n-2, n-1) |
| 14 | `/SCN_BDD/INSERN/historique` | `CertDCInsern_2023_2024` | `CertDCInsern_2023_2024.csv` | `cert_dc_insern_2023_2024.csv` | Death certs 2023-2024 |
| 15 | `/SCN_BDD/DIAMANT/pour le script etl` | `sa_esms` | `sa_esms_20251026.csv` | `sa_esms.csv` | DIAMANT ESMS data |
| 16 | `/SCN_BDD/DIAMANT/pour le script etl` | `sa_pmsi` | `sa_pmsi_20251026.csv` | `sa_pmsi.csv` | DIAMANT PMSI data |
| 17 | `/SCN_BDD/DIAMANT/pour le script etl` | `sa_rpu` | `sa_rpu_20251026.csv` | `sa_rpu.csv` | DIAMANT RPU data |
| 18 | `/SCN_BDD/DIAMANT/pour le script etl` | `sa_usld` | `sa_usld_20251026.csv` | `sa_usld.csv` | DIAMANT USLD data |
| 19 | `/SCN_BDD/HUBEE` | `sa_hubee` | `sa_hubee_20251026.csv` | `sa_hubee.csv` | HUBEE data |
| 20 | `/SCN_BDD/INSEE` | `DC_det` | `DC_det_2024.csv` | `dc_det.csv` | Death certificates detail |
| 21 | `/SCN_BDD/INSEE/historique` | `sa_insee_n_2_n_3` | `sa_insee_n_2_n_3_20241026.csv` | `sa_insee_histo.csv` | INSEE historical data |
| 22 | `/SCN_BDD/SIICEA/` | `MISSIONSPREV` | `SIICEA_MISSIONSPREV_SCN_202510200330_Production.csv` | `sa_siicea_missions_prog.csv` | Planned missions |
| 23 | `/SCN_BDD/SIICEA/` | `MISSIONSREAL_SCN` | `SIICEA_MISSIONSREAL_SCN_202510200336_Production.csv` | `sa_siicea_missions_real.csv` | Completed missions |
| 24 | `/SCN_BDD/TMP/` | `ciblage` | `ciblage_20251026.csv` | `sa_ciblage.csv` | Targeting data |
| 25 | `/SCN_BDD/TMP/` | `sa_tdb_esms` | `sa_tdb_esms_20251026.csv` | `sa_tdb_esms.csv` | ESMS dashboard data |

## Grouped by Data Source

### Core Inspection Data (SIICEA, SIREC, SIVSS)

| Remote Filename Pattern | Downloaded As | Source |
|------------------------|---------------|--------|
| `SIICEA_GROUPECIBLES_SCN_*.csv` | `sa_siicea_cibles.csv` | SIICEA |
| `SIICEA_DECISIONS_SCN_*.csv` | `sa_siicea_decisions.csv` | SIICEA |
| `SIICEA_MISSIONSPREV_SCN_*.csv` | `sa_siicea_missions_prog.csv` | SIICEA |
| `SIICEA_MISSIONSREAL_SCN_*.csv` | `sa_siicea_missions_real.csv` | SIICEA |
| `sirec_*.csv` | `sa_sirec.csv` | SIREC |
| `SIVSS_SCN_*.csv` | `sa_sivss.csv` | SIVSS |

### Reference Data (FINESS, INSEE)

| Remote Filename Pattern | Downloaded As | Source |
|------------------------|---------------|--------|
| `t-finess_*.csv` | `sa_t_finess.csv` | FINESS |
| `v_comer_*.csv` | `v_comer.csv` | INSEE |
| `v_commune_comer_*.csv` | `v_commune_comer.csv` | INSEE |
| `v_commune_depuis_*.csv` | `v_commune_depuis.csv` | INSEE |
| `v_commune_2024.csv` | `v_commune.csv` | INSEE |
| `v_departement_*.csv` | `v_departement.csv` | INSEE |
| `v_region_*.csv` | `v_region.csv` | INSEE |

### Health Data (INSERN, DIAMANT, HUBEE)

| Remote Filename Pattern | Downloaded As | Source |
|------------------------|---------------|--------|
| `DNUM_TdB_CertDc_*.csv` | `sa_insern.csv` | INSERN |
| `CertDCInsern_2023_2024.csv` | `cert_dc_insern_2023_2024.csv` | INSERN |
| `sa_insern_n_2_n_1_*.csv` | `cert_dc_insern_n2_n1.csv` | INSERN |
| `DC_det_*.csv` | `dc_det.csv` | INSEE |
| `sa_esms_*.csv` | `sa_esms.csv` | DIAMANT |
| `sa_pmsi_*.csv` | `sa_pmsi.csv` | DIAMANT |
| `sa_rpu_*.csv` | `sa_rpu.csv` | DIAMANT |
| `sa_usld_*.csv` | `sa_usld.csv` | DIAMANT |
| `sa_hubee_*.csv` | `sa_hubee.csv` | HUBEE |

### Other Data

| Remote Filename Pattern | Downloaded As | Source |
|------------------------|---------------|--------|
| `sa_insee_n_2_n_3_*.csv` | `sa_insee_histo.csv` | INSEE |
| `ciblage_*.csv` | `sa_ciblage.csv` | TMP |
| `sa_tdb_esms_*.csv` | `sa_tdb_esms.csv` | TMP |

## File Search Logic

The SFTP sync searches for files using **keyword matching**:

1. Lists all files in the specified SFTP path
2. Filters files containing the keyword in their name
3. Excludes `.gpg` and `.xlsx` files (except DIAMANT)
4. Selects the **most recent file** by modification time
5. Downloads and renames it

**Example search:**
```
Path: /SCN_BDD/SIREC
Keyword: "sirec_20250801"

Files found:
- sirec_20251020.csv (modified: 2024-10-20 14:30)
- sirec_20251026.csv (modified: 2024-10-26 08:15) ← SELECTED
- sirec_20251026_backup.csv (modified: 2024-10-26 07:00)

Downloaded as: sa_sirec.csv
```

## Comparison: Manual vs SFTP

| Method | Remote File | Action | Final Name | Location |
|--------|-------------|--------|------------|----------|
| **Manual** | `SIVSS_SCN_202510070200_prod.csv` | Copy → Manual rename | `sa_sivss.csv` | `input/staging/` |
| **SFTP** | `SIVSS_SCN_202510070200_prod.csv` | Download → Auto rename | `sa_sivss.csv` | `input/staging/` |

**Key difference:** SFTP does both steps automatically in one operation!

## Usage Examples

### Example 1: Download All Files

```bash
cd DBT/anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
```

**Result:** All 25 files downloaded and renamed to `input/staging/`

### Example 2: Check What Would Be Downloaded

```bash
# SSH into SFTP server manually to see available files
sftp -i ~/.ssh/id_rsa username@sftp.host

# Navigate to check files
cd /SCN_BDD/SIREC
ls -lt sirec*

# Check latest SIVSS file
cd /SCN_BDD/SIVSS
ls -lt SIVSS_SCN*
```

### Example 3: Download Only Specific Files

To download only specific files, you would need to modify `metadata.yml` and comment out unwanted files:

```yaml
files_to_download:
  - path: "/SCN_BDD/SIREC"
    keyword: "sirec_20250801"
    file: "sa_sirec.csv"
  # - path: "/SCN_BDD/SIVSS"      # Commented out - won't download
  #   keyword: "SIVSS_SCN"
  #   file: "sa_sivss.csv"
```

## Files You Currently Have

Based on your `ingestion/` directory, you have:

| Your File | SFTP Equivalent | Renamed To |
|-----------|----------------|------------|
| `ingestion/SIREC/sirec_20251022.csv` | Remote: `sirec_20251026.csv` | `sa_sirec.csv` |
| `ingestion/SIVSS/SIVSS_SCN_202510070200_prod.csv` | Remote: `SIVSS_SCN_*.csv` | `sa_sivss.csv` |
| `ingestion/SIICEA/SIICEA_DECISIONS_SCN_*.csv` | Remote: `SIICEA_DECISIONS_SCN_*.csv` | `sa_siicea_decisions.csv` |
| `ingestion/SIICEA/SIICEA_GROUPECIBLES_SCN_*.csv` | Remote: `SIICEA_GROUPECIBLES_SCN_*.csv` | `sa_siicea_cibles.csv` |
| `ingestion/SIICEA/SIICEA_MISSIONSPREV_SCN_*.csv` | Remote: `SIICEA_MISSIONSPREV_SCN_*.csv` | `sa_siicea_missions_prog.csv` |
| `ingestion/SIICEA/SIICEA_MISSIONSREAL_SCN_*.csv` | Remote: `SIICEA_MISSIONSREAL_SCN_*.csv` | `sa_siicea_missions_real.csv` |

**You have 6 files.** SFTP would download **25 files total**.

## Expected Output After SFTP Download

After running with `--use-sftp`, your `input/staging/` directory will contain:

```
input/staging/
├── sa_insern.csv
├── sa_siicea_cibles.csv
├── sa_siicea_decisions.csv
├── sa_sirec.csv
├── sa_sivss.csv
├── sa_t_finess.csv
├── v_comer.csv
├── v_commune_comer.csv
├── v_commune_depuis.csv
├── v_commune.csv
├── v_departement.csv
├── v_region.csv
├── cert_dc_insern_n2_n1.csv
├── cert_dc_insern_2023_2024.csv
├── sa_esms.csv
├── sa_pmsi.csv
├── sa_rpu.csv
├── sa_usld.csv
├── sa_hubee.csv
├── dc_det.csv
├── sa_insee_histo.csv
├── sa_siicea_missions_prog.csv
├── sa_siicea_missions_real.csv
├── sa_ciblage.csv
└── sa_tdb_esms.csv
```

**Total: 25 CSV files, all correctly named for pipeline ingestion.**

## Logs

When SFTP download runs, you'll see logs like:

```
================================================================================
📥 STEP 1: Downloading files from SFTP...
================================================================================
Connecting with private key authentication...
Trying to load RSA private key from /Users/beatrice/.ssh/id_rsa
✅ SFTP connection established with private key
Connexion SFTP établie.

Searching for latest file in /SCN_BDD/SIREC matching 'sirec_20250801'...
Found: sirec_20251026.csv
Downloading: /SCN_BDD/SIREC/sirec_20251026.csv → input/staging/sa_sirec.csv
✅ Downloaded and renamed successfully

Searching for latest file in /SCN_BDD/SIVSS matching 'SIVSS_SCN'...
Found: SIVSS_SCN_202510070200_prod.csv
Downloading: /SCN_BDD/SIVSS/SIVSS_SCN_202510070200_prod.csv → input/staging/sa_sivss.csv
✅ Downloaded and renamed successfully

[... 23 more files ...]

✅ SFTP download complete - files already renamed to sa_*.csv format!
```

## Summary

**Question:** Which filenames are expected when using SFTP?

**Answer:** 25 files total (see table above)

**Question:** What is the filename renaming?

**Answer:** Each remote file is renamed during download based on the `file:` parameter in `metadata.yml`. For example:
- `sirec_20251026.csv` → `sa_sirec.csv`
- `SIVSS_SCN_202510070200_prod.csv` → `sa_sivss.csv`
- `SIICEA_DECISIONS_SCN_202510200320_Production.csv` → `sa_siicea_decisions.csv`

**No manual renaming needed** - SFTP download handles it automatically! ✅
