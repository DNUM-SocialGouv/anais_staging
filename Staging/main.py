import argparse
from pipeline.duckdb_pipeline import DuckDBPipeline
from pipeline.sftp_sync import SFTPSync
from pipeline.postgres_loader import PostgreSQLLoader
from pipeline.standardize_data import standardize_all_csv_columns
import subprocess
import logging
import os
from pathlib import Path

# Configuration du logger DBT
os.makedirs("logs", exist_ok=True)
dbt_logger = logging.getLogger("dbt_logger")
file_handler = logging.FileHandler("logs/dbt.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
dbt_logger.addHandler(file_handler)
dbt_logger.setLevel(logging.INFO)


def run_dbt(project_dir: str = "./Staging/dbtStaging", profiles_dir: str = "."):
    try:
        project_path = Path(project_dir).resolve()
        profiles_path = Path(profiles_dir).resolve()

        result = subprocess.run(
            ["dbt",
            "run",
            "--project-dir", project_path,
            "--profiles-dir", profiles_path,
            ],
            # cwd=project_dir,
            capture_output=True,
            text=True,
            check=True
        )
        dbt_logger.info("dbt run terminé avec succès")
        dbt_logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        dbt_logger.error("Erreur lors du dbt run :")
        dbt_logger.error(e.stderr)

def main(env: str):
    sftp = SFTPSync()
    sftp.download_all()
    standardize_all_csv_columns("input/")
    if env == "anais":
        
        pg_loader = PostgreSQLLoader()
        pg_loader.execute_create_sql_files()
        pg_loader.load_all_csv_from_input()
        # run_dbt()

        # Export des tables listées dans .env
        pg_loader.export_tables_from_env()
        
    if env == "local":
        
        loader = DuckDBPipeline()
        loader.run()
        run_dbt()
        loader.export_to_csv()
        loader.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=["local", "anais"], default="local", help="Environnement d'exécution")
    args = parser.parse_args()

    main(args.env)