#!/usr/bin/env python3
"""
SFTP Download with Filename Tracking

Wrapper around SFTP download that captures original filenames for date tracking.
"""

import os
from typing import Dict, List
from logging import Logger


class SFTPWithTracking:
    """
    Wrapper for SFTP download that tracks original filenames.

    This class wraps the existing SFTP download functionality and captures
    the mapping of original SFTP filenames to local renamed files.
    """

    def __init__(self, sftp_client, logger: Logger):
        """
        Initialize SFTP tracking wrapper.

        Args:
            sftp_client: Existing SFTP client (SFTPSync or SFTPSyncWithKey)
            logger: Logger instance
        """
        self.sftp = sftp_client
        self.logger = logger
        self.filename_mapping: Dict[str, str] = {}  # local_filename -> original_filename

    def download_all_with_tracking(self, files_list: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Download files from SFTP and track original filenames.

        Args:
            files_list: List of dicts with 'path', 'keyword', 'file' keys

        Returns:
            Dict mapping local_filename to original_filename

        Example:
            >>> files_list = [
            ...     {'path': '/SCN_BDD/SIVSS', 'keyword': 'SIVSS_SCN', 'file': 'sa_sivss.csv'},
            ...     {'path': '/SCN_BDD/SIREC', 'keyword': 'sirec', 'file': 'sa_sirec.csv'}
            ... ]
            >>> mapping = sftp_tracker.download_all_with_tracking(files_list)
            >>> print(mapping)
            {
                'sa_sivss.csv': 'SIVSS_SCN_202510070200_prod.csv',
                'sa_sirec.csv': 'sirec_20251026.csv'
            }
        """
        self.logger.info("=" * 80)
        self.logger.info("📥 SFTP Download with Filename Tracking")
        self.logger.info("=" * 80)

        # Connect to SFTP
        self.sftp.connect()

        # First pass: Track filenames before download
        for item in files_list:
            remote_dir = item['path']
            keyword = item['keyword']
            local_filename = item['file']

            self.logger.info(f"🔍 Searching for '{keyword}' in {remote_dir}")

            # Get latest file from SFTP
            latest_file = self.sftp.get_latest_file(remote_dir, keyword)

            if latest_file:
                original_filename = latest_file.filename

                # Only track .csv files (ignore .csv.gpg, .xlsx, etc.)
                if original_filename.endswith('.csv'):
                    self.filename_mapping[local_filename] = original_filename
                    self.logger.info(f"📄 Found: {original_filename}")
                    self.logger.info(f"   → Will be saved as: {local_filename}")
                else:
                    self.logger.warning(f"⚠️  Skipping non-CSV file: {original_filename}")
                    self.logger.warning(f"   Only .csv files are tracked (ignoring .gpg, .xlsx, etc.)")
            else:
                self.logger.warning(f"⚠️  No file found for '{keyword}' in {remote_dir}")

        # Close connection (will be reopened by download_all)
        self.sftp.close()

        # Second pass: Actually download the files
        self.logger.info("")
        self.logger.info("📥 Downloading files...")
        self.sftp.download_all(files_list)

        self.logger.info("=" * 80)
        self.logger.info(f"✅ Tracked {len(self.filename_mapping)} file mappings")
        self.logger.info("=" * 80)

        return self.filename_mapping

    def get_filename_mapping(self) -> Dict[str, str]:
        """
        Get the mapping of local filenames to original SFTP filenames.

        Returns:
            Dict mapping local_filename to original_filename
        """
        return self.filename_mapping.copy()

    def get_tracked_files(self) -> List[str]:
        """
        Get list of tracked local filenames.

        Returns:
            List of local filenames
        """
        return list(self.filename_mapping.keys())


def track_manual_files(input_dir: str, logger: Logger) -> Dict[str, str]:
    """
    For manually placed files, use current filenames as original names.

    This is a fallback when files are manually copied without SFTP download.

    Args:
        input_dir: Directory containing input CSV files
        logger: Logger instance

    Returns:
        Dict mapping local_filename to original_filename (same in this case)
    """
    logger.info("=" * 80)
    logger.info("📁 Manual File Tracking (no SFTP download)")
    logger.info("=" * 80)

    # Files we care about for date tracking
    tracked_files = [
        'sa_sivss.csv',
        'sa_sirec.csv',
        'sa_siicea_decisions.csv',
        'sa_siicea_missions_real.csv',
    ]

    filename_mapping = {}

    for filename in tracked_files:
        filepath = os.path.join(input_dir, filename)
        if os.path.exists(filepath):
            # For manual files, original = local (can't extract date)
            filename_mapping[filename] = filename
            logger.info(f"📄 Found: {filename} (manual placement)")
        else:
            logger.warning(f"⚠️  Missing: {filename}")

    logger.info("=" * 80)
    logger.info(f"✅ Tracked {len(filename_mapping)} manual files")
    logger.info("=" * 80)

    return filename_mapping


if __name__ == "__main__":
    # This module is meant to be imported, not run directly
    print("This module should be imported, not run directly.")
    print("See run_local_with_sftp.py for usage example.")
