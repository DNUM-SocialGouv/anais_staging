
# Packages
import os
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
from pathlib import Path

# Modules
from pipeline.csv_management import csv_pipeline
from pipeline.database_pipeline import DataBasePipeline

# Chargement des variables d’environnement
load_dotenv()

# Configuration du logger PostgreSQL
os.makedirs("logs", exist_ok=True)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="logs/postgres_loader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Classe DuckDBPipeline qui gère les actions relatives à une database postgres
class PostgreSQLLoader(DataBasePipeline):
    def __init__(self,
                 db_config: dict,
                 sql_folder: str = "Staging/output_sql/",
                 csv_folder_input: str = "input/",
                 csv_folder_output: str = "output/"
                 ):
        """
        Initialisation de la base DuckDB avec les répertoires des fichiers.

        Parameters
        ----------
        sql_folder : str, optional
            Répertoire des fichiers SQL CREATE TABLE, by default "Staging/output_sql/"
        csv_folder_input : str, optional
            Répertoire des fichiers csv importés, by default "input/"
        csv_folder_output : str, optional
            Répertoire des fichiers csv exportés, by default "output/"
        db_config : dict
            Répertoire de la base duckdb, by default 'data/duckdb_database.duckdb'
        """
        super().__init__(sql_folder=sql_folder,
                         csv_folder_input=csv_folder_input,
                         csv_folder_output=csv_folder_output)

        self.typedb = "postgres"
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.user = db_config["user"]
        self.password = db_config["password"]
        self.database = db_config["dbname"]
        self.schema = db_config["schema"]
        self.engine = self.init_engine()

    def init_engine(self):
        try:
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(url)
            logging.info("Connexion PostgreSQL établie avec succès.")
            return engine
        except Exception as e:
            logging.error(f"Erreur de connexion PostgreSQL : {e}")
            raise

    def postgres_drop_table(self, conn, query_params: str):
        """
        Supprime une table et les vues qui lui sont liées.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Transaction
            Connexion à la base de données.
        table_name : str
            Nom de la table à supprimer.
        """
        table_name = query_params['table']

        views = conn.execute(text("""
            SELECT DISTINCT dependent_ns.nspname, dependent_view.relname
            FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class AS dependent_view ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class AS base_table ON pg_depend.refobjid = base_table.oid
            JOIN pg_namespace AS dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
            WHERE base_table.relname = :table
        """), {"table": table_name}).fetchall()

        for schema, view in views:
            # Suppression des vues liées à la table
            logging.info(f"🗑 Vue '{view}' existante → suppression totale (DROP VIEW)")
            conn.execute(text(f'DROP VIEW IF EXISTS "{schema}"."{view}" CASCADE'))

        # Suppression de la table
        logging.info(f"🗑 Table '{table_name}' existante → suppression totale (DROP TABLE)")
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

    def create_table(self, sql_query: str, query_params: str):
        if self.is_table_exist(query_params):
            with self.engine.begin() as conn:
                self.postgres_drop_table(conn, query_params)
            conn.execute(sql_query)

    def get_postgres_schema(self, conn, table_name):
        inspector = inspect(conn)
        columns = inspector.get_columns(table_name, schema=self.schema)
        schema_df = pd.DataFrame(columns)
        schema_df = schema_df.rename(columns={"name": "column_name", "type": "column_type"})
        return schema_df
    
    def is_table_exist(self, query_params: dict) -> bool:
        """
        Indique si la table existe ou non.

        Parameters
        ----------
        query_params : dict
            Paramètres à injecter dans la requête SQL.

        Returns
        -------
        bool
            True si la table existe (et non vide si applicable), False sinon.
        """
        with self.engine.begin() as conn: 
            table_exists = conn.execute(text(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_name = :table
                )
                """), query_params).scalar()
            
            if table_exists:
                # logging.warning(f"✅ La table '{table_name}' existe déjà.")
                return True
            else:
                logging.warning(f"❌ La table '{query_params['table']}' du schéma {query_params['schema']} n'existe pas.")
                return False

    def load_csv_file(self, csv_file: Path):
        
        # table_name = os.path.splitext(os.path.basename(csv_file))[0]
        logging.info(f"📥 Chargement du fichier : {csv_file}")
        table_name = csv_file.stem
        query_params = {"schema": self.schema, "table": table_name}

        try:
            if not self.is_table_exist(query_params):
                logging.warning(f"Table {table_name} non trouvée, impossible de charger {csv_file.name}")
                return

            with self.engine.begin() as conn:
                schema_df = self.get_postgres_schema(conn, table_name)

                # Chargement des csv et datamanagement
                df = csv_pipeline(csv_file, schema_df)

                # Création de la table avec la structure du CSV
                logging.info(f"🆕 Injection dans la table '{table_name}' à partir du CSV {csv_file}")
                df.to_sql(
                    table_name,
                    conn,
                    if_exists="append",
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                logging.info(f"✅ Table '{table_name}' créée et remplie avec succès ({csv_file})")

        except Exception as e:
            logging.error(f"❌ Erreur pour le fichier {csv_file} → {e}")

    def _fetch_df(self, table_name):
        with self.engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {self.schema}"))
            return pd.read_sql_table(table_name, conn)
