import argparse
from pipeline.duckdb_pipeline import DuckDBPipeline
from pipeline.sftp_sync import SFTPSync
from pipeline.postgres_loader import PostgreSQLLoader
from pipeline.standardize_data import standardize_all_csv_columns
from pipeline.loadfiles import load_colnames_YAML
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

folder_name = 'certdc'


def run_dbt(profile: str, target: str, view_directory:str, project_dir: str = "./Staging/dbtStaging", profiles_dir: str = "."):
    try:
        project_path = Path(project_dir).resolve()
        profiles_path = Path(profiles_dir).resolve()

        result = subprocess.run(
            ["dbt",
            "run",
            "--project-dir", str(project_path),
            "--profiles-dir", str(profiles_path),
            "--profile", profile,
            "--target", target,
            "--select", f"base {view_directory}"
            ],
            capture_output=True,
            text=True,
            check=True
        )
        dbt_logger.info("dbt run terminé avec succès")
        dbt_logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        dbt_logger.error("Erreur lors du dbt run :")
        dbt_logger.error(e.stdout)


def main(env: str, profile: str):
    PROJECT_PARAMS = load_colnames_YAML("file_names.yml", profile)
    if env == "anais":
        # # Récupération des fichiers sur le sftp
        # sftp = SFTPSync()
        # sftp.download_all(PROJECT_PARAMS["input_file"])
        # standardize_all_csv_columns(PROJECT_PARAMS["input_directory"])

        # Remplissage des tables de la base postgres
        pg_loader = PostgreSQLLoader(csv_folder_input=PROJECT_PARAMS["input_directory"], csv_folder_output=PROJECT_PARAMS["output_directory"])
        # pg_loader.run()

        # Création des vues et export
        run_dbt(profile=profile, target="anais", view_directory=PROJECT_PARAMS["view_directory"])
        pg_loader.export_tables_from_env(PROJECT_PARAMS["views"], PROJECT_PARAMS["output_directory"])
        
    if env == "local":
         # Remplissage des tables de la base DuckDBP
        loader = DuckDBPipeline(csv_folder_input=PROJECT_PARAMS["input_directory"], csv_folder_output=PROJECT_PARAMS["output_directory"])
        loader.run()
        loader.close()

        # Création des vues et export
        run_dbt(profile=profile, target="local", view_directory=PROJECT_PARAMS["view_directory"])
        loader = DuckDBPipeline(csv_folder_input=PROJECT_PARAMS["input_directory"], csv_folder_output=PROJECT_PARAMS["output_directory"])
        loader.export_to_csv(PROJECT_PARAMS["views"])
        loader.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=["local", "anais"], default="local", help="Environnement d'exécution")
    parser.add_argument("--profile", choices=["dbtStaging", "dbtCertDC"], default="dbtStaging", help="Profile dbt d'exécution")
    args = parser.parse_args()

    main(args.env, args.profile)