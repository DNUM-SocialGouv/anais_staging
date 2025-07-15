# Packages
import os
from pathlib import Path
from datetime import date
import argparse
import subprocess
import logging
from typing import Literal
from dotenv import load_dotenv

# Modules
from pipeline.duckdb_pipeline import DuckDBPipeline
from pipeline.sftp_sync import SFTPSync
from pipeline.postgres_loader import PostgreSQLLoader
from pipeline.load_yml import load_metadata_YAML
from pipeline.logging_management import setup_logger

# dbt_logger = setup_logger("DBT", "logs/dbt.log")

# Configuration du logger DBT
os.makedirs("logs", exist_ok=True)
dbt_logger = logging.getLogger("dbt_logger")
file_handler = logging.FileHandler("logs/dbt.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
dbt_logger.addHandler(file_handler)
dbt_logger.setLevel(logging.INFO)

# Date du jour
today = date.strftime(date.today(), "%Y_%m_%d")
load_dotenv()

def run_dbt(profile: str, target: Literal["local", "anais"], view_directory: str, project_dir: str = "./Staging/dbtStaging", profiles_dir: str = "."):
    """
    Fonction exécutant la commande 'dbt run' avec les différentes options.
    Exécute obligatoirement le répertoire 'base' dans les modèles dbt, ainsi que le répertoire choisi.

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    target : Literal["local", "anais"]
        Choix de la base à utiliser : local ou anais.
    view_directory : str
        Répertoire existant dans '/Staging/dbtStaging/models' contenant les vues à exécuter.
    project_dir : str, optional
        Répertoire du projet dbt (contenant le 'dbt_project.yml'), by default "./Staging/dbtStaging"
    profiles_dir : str, optional
        Répertoire du projet dbt (contenant le 'profiles.yml'), by default "."
    """
    try:
        project_path = str(Path(project_dir).resolve())
        profiles_path = str(Path(profiles_dir).resolve())

        result = subprocess.run(
            ["dbt",
             "run",
             "--project-dir", project_path,
             "--profiles-dir", profiles_path,
             "--profile", profile,
             "--target", target,
             "--select", f"base {view_directory}"
             ],
            capture_output=True,
            text=True,
            check=True
        )
        dbt_logger.info(f"✅ Dbt run de {view_directory} terminé avec succès")
        dbt_logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        dbt_logger.error("❌ Erreur lors du dbt run :")
        dbt_logger.error(e.stdout)


def main(env: str, profile: str, metadata: str = "metadata.yml"):
    """
    Fonction d'exécution de la Pipeline :
        1. Récupération des csv (soit du SFTP, soit en local)
        2. Standardisation des noms de colonnes
        3. Connexion à la base de données (soit postgres, soit duckdb selon l'env choisit)
        4. Création des tables
        5. Remplissage des tables par les csv
        6. Création des vues par un dbt run
        7. Export des vues au format csv
        8. Export des vues (en csv) vers le SFTP (si env anais seulement)

    Parameters
    ----------
    env : str
        Choix de la base à utiliser : local (duckdb) ou anais (postgres).
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    metadata : str, optional
        Nom du fichier de metadata contenant les informations relatives à chaque profile, by default "metadata.yml".
    """
    PROJECT_PARAMS = load_metadata_YAML(metadata, profile)
    FILES_TO_DOWNLOAD = load_metadata_YAML(metadata, 'tables')
    DB_CONFIG = load_metadata_YAML("profiles.yml", profile, ".")["outputs"][env]

    if env == "anais":
        # Récupération des fichiers sur le sftp
        sftp = SFTPSync()
        sftp.download_all(FILES_TO_DOWNLOAD["files_to_download"])

        # Remplissage des tables de la base postgres
        pg_loader = PostgreSQLLoader(
            db_config=DB_CONFIG,
            sql_folder=PROJECT_PARAMS["output_sql_directory"],
            csv_folder_input=PROJECT_PARAMS["input_directory"],
            csv_folder_output=PROJECT_PARAMS["output_directory"])
        pg_loader.run()

        # Création des vues et export
        run_dbt(profile=profile, target="anais", view_directory=PROJECT_PARAMS["view_directory"])
        pg_loader.export_csv(PROJECT_PARAMS["views"], date=today)
        pg_loader.close()
        sftp.upload_file_to_sftp(PROJECT_PARAMS["views"], PROJECT_PARAMS["output_directory"], PROJECT_PARAMS["remote_directory"], date=today)

    if env == "local":
        # Remplissage des tables de la base DuckDBP
        loader = DuckDBPipeline(
            db_path=DB_CONFIG["path"],
            sql_folder=PROJECT_PARAMS["output_sql_directory"],
            csv_folder_input=PROJECT_PARAMS["input_directory"],
            csv_folder_output=PROJECT_PARAMS["output_directory"]
            )
        loader.run()
        loader.close()

        # Création des vues et export
        run_dbt(profile=profile, target="local", view_directory=PROJECT_PARAMS["view_directory"])
        loader = DuckDBPipeline(
            db_path=DB_CONFIG["path"],
            sql_folder=PROJECT_PARAMS["output_sql_directory"],
            csv_folder_input=PROJECT_PARAMS["input_directory"],
            csv_folder_output=PROJECT_PARAMS["output_directory"]
            )
        loader.export_csv(PROJECT_PARAMS["views"], date=today)
        loader.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=["local", "anais"], default="local", help="Environnement d'exécution")
    parser.add_argument("--profile", choices=["Staging", "Helios", "InspectionControle", "Matrice_PA", "Matrice_PH", "CertDC"], default="Staging", help="Profile dbt d'exécution")
    args = parser.parse_args()

    main(args.env, args.profile)
