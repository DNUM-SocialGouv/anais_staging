#!/usr/bin/env python3
"""
File Date Tracker for ANAIS Pipeline

Tracks input file dates in database for output filename generation.
"""

import duckdb
from datetime import datetime, date
from typing import Dict, Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)


class FileDateTracker:
    """Track input file dates in database."""

    def __init__(self, db_connection: duckdb.DuckDBPyConnection):
        """
        Initialize file date tracker.

        Args:
            db_connection: DuckDB database connection
        """
        self.conn = db_connection

    def create_table(self):
        """Create input_files_date table if it doesn't exist."""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS input_files_date (
                    id INTEGER PRIMARY KEY,
                    source_system VARCHAR NOT NULL,
                    file_type VARCHAR NOT NULL,
                    original_filename VARCHAR NOT NULL,
                    local_filename VARCHAR NOT NULL,
                    extracted_date DATE NOT NULL,
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    pipeline_run_id VARCHAR
                )
            """)

            # Create index for faster queries
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_date_type_timestamp
                ON input_files_date(file_type, ingestion_timestamp DESC)
            """)

            logger.info("✅ input_files_date table ready")
        except Exception as e:
            logger.error(f"❌ Failed to create input_files_date table: {e}")
            raise

    def insert_file_date(self,
                         source_system: str,
                         file_type: str,
                         original_filename: str,
                         local_filename: str,
                         extracted_date: date,
                         pipeline_run_id: Optional[str] = None):
        """
        Insert file date record.

        Args:
            source_system: Source system name ('SIVSS', 'SIREC', 'SIICEA')
            file_type: File type ('sivss', 'sirec', 'siicea_decisions', etc.)
            original_filename: Original SFTP filename
            local_filename: Local renamed filename
            extracted_date: Date extracted from filename
            pipeline_run_id: Optional pipeline run identifier
        """
        try:
            # Get next ID
            result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM input_files_date").fetchone()
            next_id = result[0] if result else 1

            self.conn.execute("""
                INSERT INTO input_files_date
                (id, source_system, file_type, original_filename, local_filename,
                 extracted_date, pipeline_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [next_id, source_system, file_type, original_filename, local_filename,
                  extracted_date, pipeline_run_id])

            logger.info(f"✅ Tracked: {file_type} → {extracted_date} (from {original_filename})")
        except Exception as e:
            logger.error(f"❌ Failed to track {file_type}: {e}")
            # Don't raise - tracking should not block pipeline

    def insert_batch(self, file_data: List[Tuple[str, str, str, str, date, Optional[str]]]):
        """
        Insert multiple file date records at once.

        Args:
            file_data: List of tuples (source_system, file_type, original_filename,
                      local_filename, extracted_date, pipeline_run_id)
        """
        if not file_data:
            logger.info("No file dates to insert")
            return

        try:
            # Get starting ID
            result = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM input_files_date").fetchone()
            next_id = result[0] if result else 1

            # Prepare data with IDs
            rows_with_ids = [
                (next_id + i,) + row
                for i, row in enumerate(file_data)
            ]

            self.conn.executemany("""
                INSERT INTO input_files_date
                (id, source_system, file_type, original_filename, local_filename,
                 extracted_date, pipeline_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows_with_ids)

            logger.info(f"✅ Tracked {len(file_data)} files in batch")
        except Exception as e:
            logger.error(f"❌ Failed to track batch: {e}")

    def get_latest_date(self, file_type: str) -> Optional[date]:
        """
        Get the most recent extracted date for a file type.

        Args:
            file_type: File type to query

        Returns:
            datetime.date object or None
        """
        try:
            result = self.conn.execute("""
                SELECT extracted_date
                FROM input_files_date
                WHERE file_type = ?
                ORDER BY ingestion_timestamp DESC
                LIMIT 1
            """, [file_type]).fetchone()

            return result[0] if result else None
        except Exception as e:
            logger.error(f"❌ Failed to get latest date for {file_type}: {e}")
            return None

    def get_all_latest_dates(self) -> Dict[str, date]:
        """
        Get the most recent extracted dates for all file types.

        Returns:
            Dict mapping file_type to extracted_date
        """
        try:
            results = self.conn.execute("""
                WITH RankedDates AS (
                    SELECT
                        file_type,
                        extracted_date,
                        ingestion_timestamp,
                        ROW_NUMBER() OVER (PARTITION BY file_type ORDER BY ingestion_timestamp DESC) as rn
                    FROM input_files_date
                )
                SELECT file_type, extracted_date
                FROM RankedDates
                WHERE rn = 1
            """).fetchall()

            return {row[0]: row[1] for row in results}
        except Exception as e:
            logger.error(f"❌ Failed to get all latest dates: {e}")
            return {}

    def get_file_history(self, file_type: str, limit: int = 10) -> List[Tuple]:
        """
        Get ingestion history for a file type.

        Args:
            file_type: File type to query
            limit: Maximum number of records to return

        Returns:
            List of tuples (original_filename, extracted_date, ingestion_timestamp)
        """
        try:
            results = self.conn.execute("""
                SELECT original_filename, extracted_date, ingestion_timestamp
                FROM input_files_date
                WHERE file_type = ?
                ORDER BY ingestion_timestamp DESC
                LIMIT ?
            """, [file_type, limit]).fetchall()

            return results
        except Exception as e:
            logger.error(f"❌ Failed to get history for {file_type}: {e}")
            return []

    def print_summary(self):
        """Print summary of tracked files."""
        try:
            results = self.conn.execute("""
                SELECT
                    source_system,
                    file_type,
                    MAX(extracted_date) as latest_date,
                    COUNT(*) as record_count,
                    MAX(ingestion_timestamp) as last_ingestion
                FROM input_files_date
                GROUP BY source_system, file_type
                ORDER BY source_system, file_type
            """).fetchall()

            if not results:
                logger.info("📊 No files tracked yet")
                return

            logger.info("📊 File Date Tracking Summary:")
            logger.info("=" * 80)
            for row in results:
                source, file_type, latest_date, count, last_ingestion = row
                logger.info(f"  {source:8} | {file_type:25} | Date: {latest_date} | Records: {count:3} | Last: {last_ingestion}")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"❌ Failed to print summary: {e}")


if __name__ == "__main__":
    # Test the tracker
    logging.basicConfig(level=logging.INFO)

    print("\n=== Testing FileDateTracker ===\n")

    # Create test database
    conn = duckdb.connect(':memory:')
    tracker = FileDateTracker(conn)

    # Create table
    tracker.create_table()

    # Insert test data
    test_data = [
        ('SIVSS', 'sivss', 'SIVSS_SCN_202510070200_prod.csv', 'sa_sivss.csv',
         date(2025, 10, 7), 'test_run_001'),
        ('SIREC', 'sirec', 'sirec_20251026.csv', 'sa_sirec.csv',
         date(2025, 10, 26), 'test_run_001'),
        ('SIICEA', 'siicea_missions_real', 'SIICEA_MISSIONSREAL_SCN_202510200336_Production.csv',
         'sa_siicea_missions_real.csv', date(2025, 10, 20), 'test_run_001'),
    ]

    tracker.insert_batch(test_data)

    # Query latest dates
    print("\n--- Latest Dates ---")
    latest_dates = tracker.get_all_latest_dates()
    for file_type, extracted_date in latest_dates.items():
        print(f"  {file_type}: {extracted_date}")

    # Print summary
    print()
    tracker.print_summary()

    conn.close()
    print("\n✅ FileDateTracker test complete")
