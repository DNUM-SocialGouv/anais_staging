
import os
import csv
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
from io import StringIO
from datetime import date
from pipeline.csv_management import ReadCsvWithDelimiter, check_missing_columns, convert_columns_type, export_to_csv
import re
from sqlalchemy import inspect

TYPE_MAPPING = {
    "INTEGER": "int",
    "BIGINT": "int",
    "FLOAT": "float",
    "DOUBLE": "float",
    "REAL": "float",
    "BOOLEAN": "bool",
    "DATE": "datetime64",
    "TIMESTAMP": "datetime64",
}

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


class PostgreSQLLoader:
    def __init__(self, sql_folder="Staging/output_sql/", csv_folder_input="input/", csv_folder_output="output/"):
        self.sql_folder = sql_folder
        self.csv_folder_input = csv_folder_input
        self.csv_folder_output = csv_folder_output
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.database = os.getenv("PG_DATABASE")
        self.schema = os.getenv("PG_SCHEMA", "public")
        self.engine = self.init_engine()
        os.makedirs(csv_folder_output, exist_ok=True)

    def init_engine(self):
        try:
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(url)
            logging.info("Connexion PostgreSQL établie avec succès.")
            return engine
        except Exception as e:
            logging.error(f"Erreur de connexion PostgreSQL : {e}")
            raise

    def find_table_name_in_sql(self, sql_file: str)-> str:
        """
        Cherche le nom de la table dans un fichier sql (seulement un fichier avec CREATE TABLE).

        Parameters
        ----------
        sql_file : str
            Fichier sql de CREATE TABLE
        
        Returns
        -------
        str
            Nom de la table.        
        """
        match = re.compile(
            r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?P<name>(\"[^\"]+\"|\w+))",
            re.IGNORECASE
        ).search(sql_file)

        if match:
            table_name = match.group("name").strip('"')
            return table_name
        else:
            return None

    def execute_drop_table(self, conn, table_name: str):
        """
        Supprime une table et les vues qui lui sont liées.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Transaction
            Connexion à la base de données. 
        table_name : str
            Nom de la table à supprimer.   
        """
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

    def verif_exist_table(self, conn, table_name: str) -> bool:
        """
        Vérifie l'existence de la table dans la base de données.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Transaction
            Connexion à la base de données. 
        table_name : str
            Nom de la table à supprimer.

        Returns
        -------
        bool
            True si la table existe, False sinon 
        """
        # Vérifie si la table existe
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """), {"schema": self.schema, "table": table_name}).scalar()

        if table_exists:
            return True
        return False

    def execute_sql_file(self, sql_file: Path) -> None:
        sql = self._read_sql_file(sql_file)
        table_name = self.find_table_name_in_sql(sql)

        if not table_name:
            logging.info(f"❌ Nom de table introuvable dans le fichier SQL : '{sql_file.name}'")
            return

        with self.engine.begin() as conn:
            self._recreate_table(conn, table_name, sql)
            logging.info(f"✅ Table créée avec succès : {sql_file.name}")


    def _read_sql_file(self, sql_file: Path) -> str:
        """
        Lit un fichier sql.

        Parameters
        ----------
        sql_file : Path
            Fichier sql à lire.
        """
        with open(sql_file, "r", encoding="utf-8") as f:
            return f.read()


    def _recreate_table(self, conn, table_name: str, sql: str) -> None:
        """
        Créer une table à partir d'un fichier sql CREATE TABLE.
        Supprime la table en amont si elle existe déjà.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Transaction
            Connexion à la base de données. 
        table_name : str
            Nom de la table à supprimer.
        sql : str
            Contenu d'un fichier sql CREATE TABLE
        """
        if self.verif_exist_table(conn, table_name):
            self.execute_drop_table(conn, table_name)
        conn.execute(text(sql))

    # def execute_create_sql_files(self, sql_folder="./Staging/output_sql/"):
    #     """
    #     Parcours l'ensemble des fichiers sql CREATE TABLE d'un répertoire.
    #     Supprime les tables si elles existent, puis exécute le fichier pour créer ces tables.

    #     Parameters
    #     ----------
    #     sql_folder : str
    #         Répertoire des fichiers sql CREATE TABLE à parcourir.
    #     """
    #     logging.info(f"Exécution des scripts SQL dans {sql_folder}")
    #     for sql_file in Path(sql_folder).glob("*.sql"):
    #         try:
    #             self.create_table_from_sql_file(sql_file)

    #         except Exception as e:
    #             logging.error(f"❌ Erreur d'exécution {sql_file.name} : {e}")

    def detect_delimiter(self, filepath, sample_size=4096):
        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                sample = f.read(sample_size)
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=";,¤")
                return dialect.delimiter
        except Exception as e:
            logging.warning(f"⚠️ Impossible de détecter le délimiteur pour {filepath} : {e} → ';' utilisé par défaut.")
            return ";"

    def read_csv_resilient(self, filepath):
        delimiters_to_try = [self.detect_delimiter(filepath), ";", ","]
        tried = set()

        for delimiter in delimiters_to_try:
            if delimiter in tried:
                continue
            tried.add(delimiter)

            try:
                df = pd.read_csv(
                    filepath,
                    delimiter=delimiter,
                    dtype=str,
                    quotechar='"',
                    encoding='utf-8-sig'
                )
                logging.info(f"✅ Lecture réussie avec le délimiteur '{delimiter}' pour {os.path.basename(filepath)}")
                return df
            except pd.errors.ParserError as e:
                logging.warning(f"⚠️ Erreur de parsing avec '{delimiter}' pour {filepath} → {e}")

        raise ValueError(f"❌ Impossible de lire le fichier CSV {filepath} avec les délimiteurs connus.")

    def read_csv_with_custom_delimiter(self, filepath):
        try:
            with open(filepath, "rb") as f:
                raw = f.read()

            decoded = raw.decode("utf-8", errors="replace")

            df = pd.read_csv(
                StringIO(decoded),
                delimiter="¤",
                dtype=str,
                engine="python",
                quoting=csv.QUOTE_NONE,
                on_bad_lines="warn"
            )

            logging.info(f"✅ Lecture réussie avec délimiteur '¤' après détection binaire : {os.path.basename(filepath)}")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur lors de la lecture de {filepath} avec délimiteur '¤' → {e}")
            raise
    
    def load_csv_file(self, csv_file):
        logging.info("Début du chargement des fichiers CSV vers PostgreSQL.")
        table_name = os.path.splitext(os.path.basename(csv_file))[0]
        logging.info(f"📥 Chargement du fichier : {csv_file}")

        try:
            with self.engine.begin() as conn:

                inspector = inspect(conn)
                columns = inspector.get_columns(table_name, schema=self.schema)
                schema_df = pd.DataFrame(columns)
                schema_df = schema_df.rename(columns={"name": "column_name", "type": "column_type"})

                # conn.execute(text(f'SET search_path TO {self.schema}'))
                
                # Chargement des csv et datamanagement
                df = ReadCsvWithDelimiter(csv_file).read_csv_files()
                standardizer = StandardizeColnames(df)
                standardizer.standardize_column_names()
                df = standardizer.df
                check_missing_columns(csv_file, df, schema_df)
                df = convert_columns_type(TYPE_MAPPING, df, schema_df)
                
                # Nettoyage des noms de colonnes
                # df.columns = df.columns.str.strip().str.replace(r"[^\w]", "_", regex=True)

                # Création de la table avec la structure du CSV
                logging.info(f"🆕 Injection dans la table '{table_name}' à partir du CSV")
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

    logging.info("✅ Chargement PostgreSQL terminé.")

    def _fetch_df(self, table_name):
        with self.engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {self.schema}"))
            return pd.read_sql_table(table_name, conn)

    def postgres_export(self, views_to_export, output_folder, date):
        for table_name, csv_name in views_to_export.items():
            if table_name:
                export_to_csv(table_name, csv_name, self._fetch_df, output_folder, date)
            else:
                logging.warning("⚠️ Aucune table spécifiée")

    # def export_tables_from_env(self, views_to_export, output_folder="output/"):
    #     """Exporte en CSV les tables listées dans PG_EXPORT_TABLES du .env"""
    #     os.makedirs(output_folder, exist_ok=True)

    #     for table_name, csv_name in views_to_export.items():
    #         if not table_name:
    #             logging.warning("⚠️ Aucune table spécifiée dans PG_EXPORT_TABLES")
    #             return
    #         try:
    #             today = date.strftime(date.today(), "%Y_%m_%d") 
    #             file_name = f'sa_{csv_name}_{today}.csv'
    #             output_path = os.path.join(output_folder, file_name)
    #             logging.info(f"📤 Export de la table '{file_name}' vers {output_path}")
    #             with self.engine.begin() as conn:
    #                 conn.execute(text(f'SET search_path TO {self.schema}'))
    #                 df = pd.read_sql_table(table_name, conn)
    #                 df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
    #             logging.info(f"✅ Table '{file_name}' exportée avec succès.")
    #         except Exception as e:
    #             logging.error(f"❌ Erreur lors de l'export de '{file_name}' → {e}")

    # def get_dataframe_from_table(self, table_name: str):
    #     with self.engine.begin() as conn:
    #         conn.execute(text(f"SET search_path TO {self.schema}"))
    #         return pd.read_sql_table(table_name, conn)

    def run(self):
        """Exécute toutes les étapes : création des tables, chargement des CSV et vérification."""
        for sql_file in Path(self.sql_folder).glob("*.sql"):
            self.execute_sql_file(sql_file)

        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            self.load_csv_file(csv_file)

        # for csv_file in Path(self.csv_folder_input).glob("*.csv"):
        #     self.check_table(csv_file.stem, print_table=False)
