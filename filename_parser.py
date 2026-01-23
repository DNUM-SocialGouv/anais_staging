#!/usr/bin/env python3
"""
Filename Parser for ANAIS Input Files

Extracts dates from SFTP filenames to track data extraction dates.
"""

import re
from datetime import datetime, date
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class FilenameParser:
    """Extract dates from ANAIS input filenames."""

    # Regex patterns for each file type
    PATTERNS = {
        'sivss': re.compile(r'SIVSS_SCN_(\d{8})\d{4}.*\.csv', re.IGNORECASE),
        'sirec': re.compile(r'sirec_(\d{8}).*\.csv', re.IGNORECASE),
        'siicea_decisions': re.compile(r'SIICEA_DECISIONS_SCN_(\d{8})\d{4}.*\.csv', re.IGNORECASE),
        'siicea_missions_real': re.compile(r'SIICEA_MISSIONSREAL_SCN_(\d{8})\d{4}.*\.csv', re.IGNORECASE),
    }

    # Mapping of local filenames to file types
    FILENAME_TO_TYPE = {
        'sa_sivss.csv': 'sivss',
        'sa_sirec.csv': 'sirec',
        'sa_siicea_decisions.csv': 'siicea_decisions',
        'sa_siicea_missions_real.csv': 'siicea_missions_real',
    }

    # Mapping of file types to source systems
    TYPE_TO_SOURCE = {
        'sivss': 'SIVSS',
        'sirec': 'SIREC',
        'siicea_decisions': 'SIICEA',
        'siicea_missions_real': 'SIICEA',
    }

    @classmethod
    def extract_date(cls, filename: str, file_type: str) -> Optional[date]:
        """
        Extract date from filename based on file type.

        Args:
            filename: Original filename (e.g., 'SIVSS_SCN_202510070200_prod.csv')
            file_type: Type of file ('sivss', 'sirec', 'siicea_decisions', 'siicea_missions_real')

        Returns:
            datetime.date object if date found, None otherwise

        Examples:
            >>> FilenameParser.extract_date('SIVSS_SCN_202510070200_prod.csv', 'sivss')
            datetime.date(2025, 10, 7)

            >>> FilenameParser.extract_date('sirec_20251026.csv', 'sirec')
            datetime.date(2025, 10, 26)

            >>> FilenameParser.extract_date('SIICEA_MISSIONSREAL_SCN_202510200336_Production.csv', 'siicea_missions_real')
            datetime.date(2025, 10, 20)
        """
        pattern = cls.PATTERNS.get(file_type)
        if not pattern:
            logger.warning(f"No pattern defined for file type: {file_type}")
            return None

        match = pattern.search(filename)
        if match:
            date_str = match.group(1)  # YYYYMMDD
            try:
                return datetime.strptime(date_str, '%Y%m%d').date()
            except ValueError as e:
                logger.error(f"Invalid date format in {filename}: {date_str} - {e}")
                return None

        logger.warning(f"Could not extract date from {filename} using pattern for {file_type}")
        return None

    @classmethod
    def get_file_type(cls, local_filename: str) -> Optional[str]:
        """
        Get file type from local filename.

        Args:
            local_filename: Local filename (e.g., 'sa_sivss.csv')

        Returns:
            File type string or None
        """
        return cls.FILENAME_TO_TYPE.get(local_filename)

    @classmethod
    def get_source_system(cls, file_type: str) -> Optional[str]:
        """
        Get source system from file type.

        Args:
            file_type: File type (e.g., 'sivss', 'sirec')

        Returns:
            Source system name or None
        """
        return cls.TYPE_TO_SOURCE.get(file_type)

    @classmethod
    def parse_file_info(cls, original_filename: str, local_filename: str) -> Optional[Tuple[str, str, date]]:
        """
        Parse complete file information.

        Args:
            original_filename: Original SFTP filename
            local_filename: Local renamed filename

        Returns:
            Tuple of (source_system, file_type, extracted_date) or None if parsing fails

        Examples:
            >>> FilenameParser.parse_file_info('SIVSS_SCN_202510070200_prod.csv', 'sa_sivss.csv')
            ('SIVSS', 'sivss', datetime.date(2025, 10, 7))
        """
        file_type = cls.get_file_type(local_filename)
        if not file_type:
            logger.warning(f"Unknown local filename: {local_filename}")
            return None

        extracted_date = cls.extract_date(original_filename, file_type)
        if not extracted_date:
            return None

        source_system = cls.get_source_system(file_type)
        if not source_system:
            logger.warning(f"Unknown source system for file type: {file_type}")
            return None

        return (source_system, file_type, extracted_date)

    @classmethod
    def parse_all_files(cls, file_mapping: Dict[str, str]) -> Dict[str, Tuple[str, str, date]]:
        """
        Parse dates from multiple files.

        Args:
            file_mapping: Dict mapping local_filename to original_filename

        Returns:
            Dict mapping local_filename to (source_system, file_type, extracted_date)

        Example:
            >>> file_mapping = {
            ...     'sa_sivss.csv': 'SIVSS_SCN_202510070200_prod.csv',
            ...     'sa_sirec.csv': 'sirec_20251026.csv'
            ... }
            >>> FilenameParser.parse_all_files(file_mapping)
            {
                'sa_sivss.csv': ('SIVSS', 'sivss', datetime.date(2025, 10, 7)),
                'sa_sirec.csv': ('SIREC', 'sirec', datetime.date(2025, 10, 26))
            }
        """
        results = {}
        for local_filename, original_filename in file_mapping.items():
            file_info = cls.parse_file_info(original_filename, local_filename)
            if file_info:
                results[local_filename] = file_info
                logger.info(f"✅ Parsed: {local_filename} → {file_info[0]} @ {file_info[2]}")
            else:
                logger.warning(f"⚠️  Could not parse: {original_filename} → {local_filename}")

        return results


if __name__ == "__main__":
    # Test the parser
    logging.basicConfig(level=logging.INFO)

    test_cases = [
        ('SIVSS_SCN_202510070200_prod.csv', 'sa_sivss.csv'),
        ('sirec_20251026.csv', 'sa_sirec.csv'),
        ('SIICEA_DECISIONS_SCN_202510200320_Production.csv', 'sa_siicea_decisions.csv'),
        ('SIICEA_MISSIONSREAL_SCN_202510200336_Production.csv', 'sa_siicea_missions_real.csv'),
    ]

    print("\n=== Testing FilenameParser ===\n")
    for original, local in test_cases:
        result = FilenameParser.parse_file_info(original, local)
        if result:
            source, file_type, extracted_date = result
            print(f"✅ {original}")
            print(f"   → Source: {source}, Type: {file_type}, Date: {extracted_date}")
        else:
            print(f"❌ {original} → Failed to parse")
        print()
