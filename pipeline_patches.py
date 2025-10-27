#!/usr/bin/env python3
"""
Pipeline Patches for ANAIS Staging

This module contains monkey patches for the installed pipeline package to fix bugs
that cannot be fixed directly in the installed package.

Usage:
    Import this module at the beginning of run_local_with_sftp.py:
        from pipeline_patches import apply_all_patches
        apply_all_patches()
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def patch_boolean_conversion():
    """
    Patch CSV boolean conversion to correctly handle lowercase 'true'/'false' strings.

    Problem:
        pandas .astype(bool) converts any non-empty string to True, including "false"

    Solution:
        Replace the boolean conversion logic with explicit string mapping

    Note on SIVSS boolean columns:
        The SIVSS table previously had 4 BOOLEAN columns (est_eigs, reclamation,
        declarant_est_anonyme, survenue_cas_collectivite) that were being incorrectly
        converted. These columns have been changed to VARCHAR(5) in the SQL schema
        (output_sql/staging/sa_sivss.sql) to preserve the lowercase string values
        'true'/'false' instead of converting them to Python booleans True/False.

        Since VARCHAR maps to 'string' type in the type_mapping, these columns will
        NOT trigger this boolean conversion patch and will remain as strings naturally.

        This patch is still useful for any other tables that have actual BOOLEAN
        columns in their SQL schema and need proper boolean conversion from CSV strings.
    """
    try:
        from pipeline.utils import csv_management

        # Store original method
        original_convert = csv_management.ColumnsManagement.convert_columns_type

        def patched_convert_columns_type(self):
            """
            Patched version of convert_columns_type that correctly handles boolean values.
            """
            logger.debug("🔧 Using patched boolean conversion logic")

            for _, row in self.schema_df.iterrows():
                col_name = row["column_name"]
                col_type = str(row["column_base_type"])
                col_length = row["column_length"]

                if col_name in self.df.columns and col_type in self.type_mapping:
                    try:
                        if self.type_mapping[col_type] in ["int", "float"]:
                            self.df[col_name] = self.df[col_name].replace(
                                {None: 0, "": 0, pd.NA: 0, "nan": 0}
                            ).astype(float).astype(self.type_mapping[col_type])

                        elif self.type_mapping[col_type] == "bool":
                            # === PATCHED BOOLEAN CONVERSION ===
                            # Create comprehensive mapping for string boolean values
                            bool_map = {
                                # String representations
                                'true': True, 'True': True, 'TRUE': True,
                                'false': False, 'False': False, 'FALSE': False,
                                # Numeric representations
                                '1': True, 1: True, 1.0: True,
                                '0': False, 0: False, 0.0: False,
                                # NULL/empty representations
                                None: False, '': False, pd.NA: False,
                                'nan': False, 'NaN': False, 'NAN': False,
                                # Additional common formats
                                'yes': True, 'Yes': True, 'YES': True,
                                'no': False, 'No': False, 'NO': False,
                                'y': True, 'Y': True,
                                'n': False, 'N': False,
                                't': True, 'T': True,
                                'f': False, 'F': False,
                            }

                            # Map values using the dictionary
                            mapped_values = self.df[col_name].map(bool_map)

                            # Check for unmapped values and log warnings
                            unmapped_mask = mapped_values.isna() & self.df[col_name].notna()
                            if unmapped_mask.any():
                                unmapped_values = self.df.loc[unmapped_mask, col_name].unique()
                                logger.warning(
                                    f"⚠️  Column '{col_name}': Found unmapped boolean values: {list(unmapped_values)}. "
                                    f"These will be converted to False."
                                )

                            # Fill unmapped values with False and convert to boolean
                            self.df[col_name] = mapped_values.fillna(False).astype(bool)
                            logger.debug(f"✅ Column '{col_name}': Boolean conversion successful")
                            # === END PATCHED BOOLEAN CONVERSION ===

                        elif self.type_mapping[col_type] == "datetime64":
                            self.df[col_name] = pd.to_datetime(
                                self.df[col_name], format="%d-%m-%Y", errors="coerce"
                            )

                        elif self.type_mapping[col_type] == "string":
                            self.df[col_name] = self.df[col_name].astype(
                                self.type_mapping[col_type]
                            ).fillna('')
                            if not pd.isna(col_length):
                                self.df[col_name] = self.df[col_name].str[:col_length]

                        else:
                            self.df[col_name] = self.df[col_name].astype(
                                self.type_mapping[col_type]
                            )

                    except ValueError as e:
                        logger.warning(f"Erreur de conversion pour {col_name}: {e}")

        # Apply the patch
        csv_management.ColumnsManagement.convert_columns_type = patched_convert_columns_type
        logger.info("✅ Boolean conversion patch applied successfully")

    except ImportError as e:
        logger.error(f"❌ Failed to apply boolean conversion patch: {e}")
        logger.error("Pipeline package not found. Make sure dependencies are installed.")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error applying boolean conversion patch: {e}")
        raise


def apply_all_patches():
    """
    Apply all monkey patches to the pipeline package.

    This function should be called at the start of run_local_with_sftp.py
    before any pipeline operations are performed.
    """
    logger.info("=" * 80)
    logger.info("🔧 Applying pipeline patches...")
    logger.info("=" * 80)

    # Apply boolean conversion patch
    patch_boolean_conversion()

    logger.info("✅ All patches applied successfully")
    logger.info("")


if __name__ == "__main__":
    # Test the patch
    logging.basicConfig(level=logging.INFO)
    apply_all_patches()
    print("\n✅ Patches applied successfully. Import this module in your pipeline script.")
