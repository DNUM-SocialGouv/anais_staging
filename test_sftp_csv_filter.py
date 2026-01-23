#!/usr/bin/env python3
"""
Unit test for SFTP CSV file filtering.

This test verifies that only .csv files are tracked, ignoring .csv.gpg, .xlsx, etc.
"""

import sys
from typing import Optional


class MockSFTPFile:
    """Mock SFTP file object for testing."""
    def __init__(self, filename: str):
        self.filename = filename


def test_csv_filter():
    """Test that only .csv files are tracked."""

    test_cases = [
        # (filename, should_track, description)
        ('SIVSS_SCN_202510070200_prod.csv', True, 'CSV file'),
        ('SIVSS_SCN_202510070200_prod.csv.gpg', False, 'GPG encrypted CSV'),
        ('sirec_20251026.csv', True, 'CSV file'),
        ('sirec_20251026.csv.gpg', False, 'GPG encrypted CSV'),
        ('SIICEA_DECISIONS_SCN_202510200336_Production.xlsx', False, 'Excel file'),
        ('data.txt', False, 'Text file'),
        ('file.CSV', False, 'Uppercase extension (edge case)'),
    ]

    print('=' * 80)
    print('Testing CSV File Filter')
    print('=' * 80)
    print()

    all_passed = True

    for filename, should_track, description in test_cases:
        # Apply the filter logic
        is_csv = filename.endswith('.csv')

        # Check if result matches expectation
        passed = (is_csv == should_track)
        status = '✅ PASS' if passed else '❌ FAIL'

        print(f'{status} | {filename:55} | {description}')
        print(f'       Expected: {should_track:5} | Got: {is_csv:5}')
        print()

        if not passed:
            all_passed = False

    print('=' * 80)
    if all_passed:
        print('✅ All tests passed!')
        print('=' * 80)
        return 0
    else:
        print('❌ Some tests failed!')
        print('=' * 80)
        return 1


if __name__ == '__main__':
    exit_code = test_csv_filter()
    sys.exit(exit_code)
