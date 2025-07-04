import duckdb
import pandas as pd
import os
from pathlib import Path
import logging
import csv
from datetime import date
from pipeline.csv_management import ReadCsvWithDelimiter, check_missing_columns, convert_columns_type
from pipeline.constantes import TYPE_MAPPING

# === Configuration du logger ===
os.makedirs("logs", exist_ok=True)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="logs/duckdb_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class DuckDBPipeline:
    def __init__(self, db_path='data/duckdb_database.duckdb', sql_folder="Staging/output_sql/", csv_folder_input="input/", csv_folder_output="output/"):
        """Initialisation du chargeur DuckDB avec les chemins de la base et des fichiers."""
        self.db_path = db_path
        self.sql_folder = sql_folder
        self.csv_folder_input = csv_folder_input
        self.csv_folder_output = csv_folder_output

        self.ensure_directories_exist()
        self.init_duckdb()
        self.conn = duckdb.connect(database=self.db_path)

    def ensure_directories_exist(self):
        """Crée les dossiers nécessaires s'ils n'existent pas."""
        for folder in [self.sql_folder, self.csv_folder_input, self.csv_folder_output]:
            os.makedirs(folder, exist_ok=True)
            logging.info(f"Dossier vérifié/créé : {folder}")

    def init_duckdb(self):
        """Vérifie si la base DuckDB existe, sinon la crée."""
        if not os.path.exists(self.db_path):
            logging.info("Création de la base DuckDB...")
            conn = duckdb.connect(self.db_path)
            conn.close()
        else:
            logging.info("La base DuckDB existe déjà.")

    def execute_sql_file(self, sql_file):
        """Exécute un fichier SQL si la table n'existe pas."""
        table_name = sql_file.stem
        result = self.conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]

        if result > 0:
            logging.info(f"Table {table_name} déjà existante, passage du fichier SQL : {sql_file.name}")
            return

        with open(sql_file, "r", encoding="utf-8") as f:
            sql_script = f.read()

        logging.info(f"Création de la table {table_name} depuis {sql_file.name}")
        try:
            self.conn.execute(sql_script)
        except duckdb.Error as e:
            logging.error(f"Erreur lors de l'exécution du SQL {sql_file.name}: {e}")


    def detect_delimiter(self, filepath, sample_size=4096):
        """Détecte automatiquement le délimiteur du fichier CSV."""
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
        """Tente de lire un CSV avec différents délimiteurs."""
        delimiters_to_try = [self.detect_delimiter(filepath), ";", ",", "¤"]
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
            except Exception as e:
                logging.warning(f"⚠️ Autre erreur avec '{delimiter}' → {e}")

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
        """Charge un fichier CSV après vérification et conversion des types."""
        table_name = csv_file.stem
        result = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]

        if result == 0:
            logging.warning(f"Table {table_name} non trouvée, impossible de charger {csv_file.name}")
            return

        schema_df = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()
        
        # Chargement des csv et datamanagement
        delimiter = ReadCsvWithDelimiter(csv_file)
        df = delimiter.read_csv_files()
        check_missing_columns(csv_file.name, df, schema_df)
        df = convert_columns_type(TYPE_MAPPING, df, schema_df)

        # Vérification de la présence de la table
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if row_count > 0:
            logging.info(f"Données déjà présentes dans {table_name}, passage du fichier CSV : {csv_file.name}")
        else:
            logging.info(f"Chargement des données dans {table_name} depuis {csv_file.name}")
            try:
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            except duckdb.Error as e:
                logging.error(f"Erreur lors du chargement de {csv_file.name}: {e}")

    def check_table(self, table_name, limit=10, print_table=True):
        """Affiche les premières lignes d'une table (dans les logs uniquement si elle existe)."""
        try:
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            """).fetchone()[0]

            if table_exists == 0:
                logging.warning(f"La table '{table_name}' n'existe pas.")
                return

            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if row_count == 0:
                logging.warning(f"La table '{table_name}' est vide.")
                return

            if print_table:
                df = self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit};").fetchdf()
                logging.info(f"{limit} lignes de '{table_name}':\n{df.to_string(index=False)}")
            return True
            
        except Exception as e:
            logging.error(f"Erreur lors de la lecture de la table '{table_name}': {e}")

    def export_to_csv(self, views_to_export):
        """ Export une table de DuckDB vers un csv"""

        for table_name, csv_name in views_to_export.items():
            # Vérification de l'existence de la table
            if not self.check_table(table_name, print_table=False):
                return

            today = date.strftime(date.today(), "%Y_%m_%d") 
            file_name = f'sa_{csv_name}_{today}.csv'
            file = os.path.join(self.csv_folder_output, file_name)

            try:
                df = self.conn.execute(f"SELECT * FROM {table_name}").df()
                df.to_csv(file, index=False)
                logging.info(f"Export réussi : {file}")
            except Exception as e:
                logging.error(f"Erreur lors de l'export de {table_name} : {e}")

    def list_tables(self):
        """Liste toutes les tables existantes dans la base DuckDB."""
        try:
            tables = self.conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()

            if not tables:
                logging.warning("Aucune table trouvée dans la base DuckDB.")
                return

            logging.info("Tables disponibles dans DuckDB :")
            for schema, table in tables:
                logging.info(f" - {schema}.{table}")

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des tables : {e}")

    def run(self):
        """Exécute toutes les étapes : création des tables, chargement des CSV et vérification."""
        for sql_file in Path(self.sql_folder).glob("*.sql"):
            self.execute_sql_file(sql_file)

        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            self.load_csv_file(csv_file)

        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            self.check_table(csv_file.stem, print_table=False)

    def close(self):
        """Ferme la connexion à la base de données."""
        self.conn.close()
        logging.info("Connexion à DuckDB fermée.")