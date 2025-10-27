#!/usr/bin/env python3
"""
Local Staging Pipeline with SFTP Download Support

This script extends the local staging pipeline to optionally download files from SFTP
before loading them into DuckDB. It combines functionality from both anais_staging_pipeline
(SFTP download) and local_staging_pipeline (DuckDB loading).

Usage:
    # Without SFTP (manual files)
    uv run run_local_with_sftp.py --env "local" --profile "Staging"

    # With SFTP (automatic download)
    uv run run_local_with_sftp.py --env "local" --profile "Staging" --use-sftp
"""

# === Packages ===
import argparse
import os
from logging import Logger
from dotenv import load_dotenv
from paramiko import Transport, SFTPClient, RSAKey, Ed25519Key, ECDSAKey
from typing import Optional

# === Apply Pipeline Patches (MUST BE BEFORE OTHER PIPELINE IMPORTS) ===
from pipeline_patches import apply_all_patches
apply_all_patches()

# === Modules ===
from pipeline.utils.config import setup_config
from pipeline.utils.load_yml import load_metadata_YAML
from pipeline.utils.logging_management import setup_logger
from pipeline.utils.sftp_sync import SFTPSync
from pipeline.database_management.duckdb_pipeline import DuckDBPipeline
from pipeline.utils.dbt_tools import dbt_exec

# === Constants ===
ENV_CHOICE = ["local"]  # Only local environment supported
PROFILE_CHOICE = ["Staging", "CertDC", "Helios", "InspectionControlePA", "InspectionControlePH", "MatricePreciblage"]
METADATA_YML = "metadata.yml"
PROFILE_YML = "profiles.yml"


# === Custom SFTP Class with Private Key Support ===
class SFTPSyncWithKey(SFTPSync):
    """
    Extended SFTPSync class that supports private key authentication.

    Inherits from pipeline.utils.sftp_sync.SFTPSync and overrides the connect() method
    to support both password and private key authentication.
    """

    def __init__(self, output_folder: str, logger: Logger):
        """Initialize SFTP connection with support for private key."""
        super().__init__(output_folder, logger)
        # Load private key path from .env
        self.private_key_path = os.getenv("SFTP_PRIVATE_KEY_PATH")
        self.private_key_passphrase = os.getenv("SFTP_PRIVATE_KEY_PASSPHRASE")

    def _load_private_key(self, key_path: str, passphrase: Optional[str] = None):
        """
        Load private key from file, trying different key types.

        Parameters
        ----------
        key_path : str
            Path to private key file
        passphrase : Optional[str]
            Passphrase for encrypted key (optional)

        Returns
        -------
        paramiko.PKey
            Loaded private key
        """
        # Expand user path (~/)
        key_path = os.path.expanduser(key_path)

        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Private key file not found: {key_path}")

        # Try different key types
        key_types = [
            (RSAKey, "RSA"),
            (Ed25519Key, "Ed25519"),
            (ECDSAKey, "ECDSA"),
        ]

        for key_class, key_name in key_types:
            try:
                self.logger.info(f"Trying to load {key_name} private key from {key_path}")
                if passphrase:
                    return key_class.from_private_key_file(key_path, password=passphrase)
                else:
                    return key_class.from_private_key_file(key_path)
            except Exception as e:
                self.logger.debug(f"Failed to load as {key_name}: {e}")
                continue

        raise ValueError(f"Could not load private key from {key_path}. Tried RSA, Ed25519, and ECDSA formats.")

    def connect(self):
        """
        Initialize SFTP connection using private key (if provided) or password.

        Priority:
        1. Private key authentication (if SFTP_PRIVATE_KEY_PATH is set)
        2. Password authentication (if SFTP_PASSWORD is set)
        """
        try:
            self.transport = Transport((self.host, self.port))

            # Try private key authentication first
            if self.private_key_path:
                self.logger.info("Connecting with private key authentication...")
                try:
                    private_key = self._load_private_key(
                        self.private_key_path,
                        self.private_key_passphrase
                    )
                    self.transport.connect(username=self.username, pkey=private_key)
                    self.sftp = SFTPClient.from_transport(self.transport)
                    self.logger.info("✅ SFTP connection established with private key")
                    return
                except Exception as e:
                    self.logger.error(f"Private key authentication failed: {e}")
                    raise

            # Fallback to password authentication
            elif self.password:
                self.logger.info("Connecting with password authentication...")
                self.transport.connect(username=self.username, password=self.password)
                self.sftp = SFTPClient.from_transport(self.transport)
                self.logger.info("✅ SFTP connection established with password")

            else:
                raise ValueError(
                    "No authentication method available. "
                    "Please set either SFTP_PRIVATE_KEY_PATH or SFTP_PASSWORD in .env file."
                )

        except Exception as e:
            self.logger.error(f"❌ SFTP connection failed: {e}")
            raise


def local_staging_pipeline_with_sftp(
    profile: str,
    config: dict,
    db_config: dict,
    logger: Logger,
    use_sftp: bool = False
):
    """
    Pipeline for Staging in local environment with optional SFTP download.

    Steps:
        1. (Optional) Download files from SFTP
        2. Connect to DuckDB Staging database
        3. Create tables and inject data
        4. Create views via DBT

    Parameters
    ----------
    profile : str
        DBT profile to use from 'profiles.yml'.
    config : dict
        Profile metadata (from metadata.yml).
    db_config : dict
        DuckDB configuration parameters (from 'profiles.yml').
    logger : Logger
        Log file.
    use_sftp : bool
        If True, download files from SFTP before processing.
    """
    # Step 1: SFTP Download (optional)
    if use_sftp:
        logger.info("=" * 80)
        logger.info("📥 STEP 1: Downloading files from SFTP...")
        logger.info("=" * 80)
        try:
            sftp = SFTPSyncWithKey(config["local_directory_input"], logger)
            sftp.download_all(config["files_to_download"])
            logger.info("✅ SFTP download complete - files already renamed to sa_*.csv format!")
            logger.info("")
        except Exception as e:
            logger.error(f"❌ SFTP download failed: {e}")
            logger.error("Make sure .env file contains SFTP credentials:")
            logger.error("  Required: SFTP_HOST, SFTP_PORT, SFTP_USERNAME")
            logger.error("  Authentication: SFTP_PRIVATE_KEY_PATH or SFTP_PASSWORD")
            logger.error("  Optional: SFTP_PRIVATE_KEY_PASSPHRASE (if key is encrypted)")
            raise
    else:
        logger.info("=" * 80)
        logger.info("📂 STEP 1: Using manual CSV files (no SFTP download)")
        logger.info("=" * 80)
        logger.info(f"Looking for files in: {config['local_directory_input']}")
        csv_count = len([f for f in os.listdir(config['local_directory_input']) if f.endswith('.csv')])
        logger.info(f"Found {csv_count} CSV files")
        logger.info("")

    # Step 2: Initialize DuckDB loader
    logger.info("=" * 80)
    logger.info("🦆 STEP 2: Initializing DuckDB connection...")
    logger.info("=" * 80)
    loader = DuckDBPipeline(
        db_config=db_config,
        config=config,
        logger=logger
    )

    # Step 3: Load data into DuckDB
    loader.connect()
    try:
        logger.info("=" * 80)
        logger.info("📊 STEP 3: Loading CSV data into DuckDB...")
        logger.info("=" * 80)
        # Check if we have files and SQL schemas
        if os.listdir(config["local_directory_input"]) and os.listdir(config["create_table_directory"]):
            loader.run()
            logger.info("✅ Data loading complete")
            logger.info("")
        else:
            logger.error(
                "❌ Cannot populate DuckDB database.\n"
                f"- Empty directories:\n"
                f"    > CSV files: {config['local_directory_input']}\n"
                f"    > SQL schemas: {config['create_table_directory']}"
            )
            raise FileNotFoundError("Missing CSV files or SQL schemas")
    finally:
        duckdb_empty = loader.is_duckdb_empty()
        loader.close()

    # Step 4: Run DBT models (if database is not empty)
    if not duckdb_empty:
        logger.info("=" * 80)
        logger.info("🔄 STEP 4: Running DBT transformations...")
        logger.info("=" * 80)
        # Create views
        dbt_exec("run", profile, "local", config["models_directory"], ".", logger, install_deps=False)
        # Run tests
        dbt_exec("test", profile, "local", config["models_directory"], ".", logger)
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Pipeline completed successfully!")
        logger.info("=" * 80)
    else:
        logger.error(f"❌ Database {db_config['path']} is empty")
        raise RuntimeError("DuckDB database is empty after loading")


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Local Staging Pipeline with optional SFTP download"
    )
    parser.add_argument(
        "--env",
        choices=ENV_CHOICE,
        default=ENV_CHOICE[0],
        help="Execution environment (only 'local' supported)"
    )
    parser.add_argument(
        "--profile",
        choices=PROFILE_CHOICE,
        default=PROFILE_CHOICE[0],
        help="DBT profile to execute"
    )
    parser.add_argument(
        "--use-sftp",
        action="store_true",
        help="Download files from SFTP before running pipeline (requires .env with SFTP credentials)"
    )
    args = parser.parse_args()

    # Setup configuration
    config_var = {
        "env_choice": ENV_CHOICE,
        "profile_choice": PROFILE_CHOICE,
        "env": args.env,
        "profile": args.profile,
        "use_sftp": args.use_sftp
    }

    # Load logger and config
    logger = setup_logger(args.env, f"logs/log_{args.env}_sftp.log")
    config = load_metadata_YAML(METADATA_YML, args.profile, logger, ".")
    db_config = load_metadata_YAML(PROFILE_YML, args.profile, logger, ".")["outputs"][args.env]

    # Print execution info
    logger.info("=" * 80)
    logger.info("🚀 LOCAL STAGING PIPELINE WITH SFTP SUPPORT")
    logger.info("=" * 80)
    logger.info(f"Environment: {args.env}")
    logger.info(f"Profile: {args.profile}")
    logger.info(f"SFTP Download: {'✅ Enabled' if args.use_sftp else '❌ Disabled (using manual files)'}")
    logger.info(f"Database: {db_config['path']}")
    logger.info("")

    # Run pipeline
    local_staging_pipeline_with_sftp(
        profile=args.profile,
        config=config,
        db_config=db_config,
        logger=logger,
        use_sftp=args.use_sftp
    )


if __name__ == "__main__":
    main()
