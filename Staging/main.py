# Packages
import os
from pathlib import Path
from datetime import date
import argparse
import subprocess
from typing import Literal
from dotenv import load_dotenv

# Modules
from pipeline.duckdb_pipeline import DuckDBPipeline
from pipeline.sftp_sync import SFTPSync
from pipeline.postgres_loader import PostgreSQLLoader
from pipeline.load_yml import load_metadata_YAML
from pipeline.logging_management import setup_logger


# Date du jour
today = date.strftime(date.today(), "%Y_%m_%d")
load_dotenv()

CHOICES=["Staging", "Helios", "InspectionControlePA", "Matrice_PA", "Matrice_PH", "CertDC"]


def run_dbt(profile: str, target: Literal["local", "anais"], project_dir: str, profiles_dir: str = ".", logger=None):
    """
    Fonction exécutant la commande 'dbt run' avec les différentes options.
    Exécute obligatoirement le répertoire 'base' dans les modèles dbt, ainsi que le répertoire choisi.

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    target : Literal["local", "anais"]
        Choix de la base à utiliser : local ou anais.
    project_dir : str, optional
        Répertoire du projet dbt à exécuter (contenant le 'dbt_project.yml')
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
             "--target", target
             ],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ Dbt run de {project_dir} terminé avec succès")
        logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        logger.error("❌ Erreur lors du dbt run :")
        logger.error(e.stdout)

def staging_pipeline(env, profile, metadata, logger=None):
    config = load_metadata_YAML(metadata, profile, logger=logger)
    db_config = load_metadata_YAML("profiles.yml", profile, ".", logger=logger)["outputs"][env]

    if env == "anais":
        # Initialisation de la config postgres
        pg_loader = PostgreSQLLoader(
            db_config=db_config,
            config=config,
            logger=logger)

        # Récupération des fichiers sur le sftp
        sftp = SFTPSync(config["local_directory_input"], logger)
        sftp.download_all(config["files_to_download"])

        # Remplissage des tables de la base postgres
        pg_loader.connect()
        pg_loader.run()
        pg_loader.close()

        # Création des vues et export
        run_dbt(profile=profile, target="anais", project_dir=config["models_directory"], logger=logger)
        
    if env == "local":
        # Initialisation de la config DuckDB
        loader = DuckDBPipeline(
            db_config=db_config,
            config=config,
            logger=logger)

        # Remplissage des tables de la base DuckDB
        loader.connect()
        loader.run()
        loader.close()

        # Création des vues et export
        run_dbt(profile=profile, target="local", project_dir=config["models_directory"], logger=logger)


def projects_pipeline(env, profile, metadata, logger=None):
    config = load_metadata_YAML(metadata, profile, logger=logger)
    db_config = load_metadata_YAML("profiles.yml", profile, ".", logger=logger)["outputs"][env]
    staging_db_config = load_metadata_YAML("profiles.yml", "Staging", ".", logger=logger)["outputs"][env]

    if env == "anais":
        # --- Staging ---
        # Initialisation de la config postgres
        pg_staging_loader = PostgreSQLLoader(
            db_config=staging_db_config,
            config=config,
            logger=logger)

        # Récupération des tables provenant de Staging
        pg_staging_loader.connect()
        pg_staging_loader.import_csv(config["input_to_upload"])
        pg_staging_loader.close()

        # --- Projet ---
        # Initialisation de la config postgres
        pg_loader = PostgreSQLLoader(
            db_config=db_config,
            config=config,
            logger=logger)

        # Remplissage des tables de la base postgres
        pg_loader.connect()
        pg_loader.run()

        # Création des vues et export
        run_dbt(profile=profile, target="anais", project_dir=config["models_directory"], logger=logger)

        # Upload les tables qui servent à la création des vues
        pg_loader.export_csv(config["input_to_upload"], date=today)
        # sftp.upload_file_to_sftp(config["input_to_upload"], config["local_directory_output"], config["remote_directory_input"], date=today)
        
        # Upload les vues
        pg_loader.export_csv(config["files_to_upload"], date=today)
        # sftp.upload_file_to_sftp(config["files_to_upload"], config["local_directory_output"], config["remote_directory_output"], date=today)
        pg_loader.close()


    if env == "local":
        # Initialisation de la config duckDB
        ddb_staging_loader = DuckDBPipeline(
            db_config=staging_db_config,
            config=config,
            logger=logger)

        # Récupération des tables provenant de Staging
        ddb_staging_loader.connect()
        ddb_staging_loader.import_csv(config["input_to_upload"])
        ddb_staging_loader.close()
        
        # Initialisation de la config DuckDB
        ddb_loader = DuckDBPipeline(
            db_config=db_config,
            config=config,
            logger=logger)

        ddb_loader.connect()
        ddb_loader.run()
        ddb_loader.close()

        # Création des vues et export
        run_dbt(profile=profile, target="local", project_dir=config["models_directory"], logger=logger)

        # Upload les vues
        ddb_loader.connect()
        ddb_loader.export_csv(config["files_to_upload"], date=today)
        ddb_loader.close()

def main(env: str, profile: str, metadata: str = "metadata.yml"):
    logger = setup_logger(env, f"logs/log_{env}.log")

    if profile == "Staging":
        staging_pipeline(env, profile, metadata, logger)
    elif profile in CHOICES and profile != "Staging":
        DB_CONFIG = load_metadata_YAML("profiles.yml", profile, ".", logger=logger)["outputs"][env]
        projects_pipeline(env, profile, metadata, logger)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=["local", "anais"], default="local", help="Environnement d'exécution")
    parser.add_argument("--profile", choices=CHOICES, default="Staging", help="Profile dbt d'exécution")
    args = parser.parse_args()

    main(args.env, args.profile)
