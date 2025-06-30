import duckdb
import pandas as pd
import os
from pathlib import Path
import logging

# === Configuration du logger ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/duckdb_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class DuckDBPipeline:
    def __init__(self, db_path='data/duckdb_database.duckdb', sql_folder="output_sql", csv_folder="input/"):
        """Initialisation du chargeur DuckDB avec les chemins de la base et des fichiers."""
        self.db_path = db_path
        self.sql_folder = sql_folder
        self.csv_folder = csv_folder

        self.ensure_directories_exist()
        self.init_duckdb()
        self.conn = duckdb.connect(database=self.db_path)

    def ensure_directories_exist(self):
        """Crée les dossiers nécessaires s'ils n'existent pas."""
        for folder in [self.sql_folder, self.csv_folder]:
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
        df = pd.read_csv(csv_file, dtype=str, delimiter=";")

        table_columns = schema_df["column_name"].tolist()
        csv_columns = df.columns.tolist()

        missing_columns = set(table_columns) - set(csv_columns)
        extra_columns = set(csv_columns) - set(table_columns)

        if missing_columns:
            logging.warning(f"Colonnes manquantes dans {csv_file.name} : {missing_columns}")
            for col in missing_columns:
                df[col] = None

        if extra_columns:
            logging.warning(f"Colonnes en trop dans {csv_file.name} : {extra_columns}")
            df = df[table_columns]

        type_mapping = {
            "INTEGER": "int",
            "BIGINT": "int",
            "FLOAT": "float",
            "DOUBLE": "float",
            "REAL": "float",
            "BOOLEAN": "bool",
            "DATE": "datetime64",
            "TIMESTAMP": "datetime64",
        }

        for _, row in schema_df.iterrows():
            col_name = row["column_name"]
            col_type = row["column_type"].split("(")[0]

            if col_name in df.columns and col_type in type_mapping:
                try:
                    if type_mapping[col_type] in ["int", "float"]:
                        df[col_name] = df[col_name].replace({None: 0, "": 0, pd.NA: 0, "nan": 0}).astype(type_mapping[col_type])
                    elif type_mapping[col_type] == "bool":
                        df[col_name] = df[col_name].replace({None: False, "": False, pd.NA: False}).astype(bool)
                    elif type_mapping[col_type] == "datetime64":
                        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                    else:
                        df[col_name] = df[col_name].astype(type_mapping[col_type])
                except ValueError as e:
                    logging.warning(f"Erreur de conversion de {col_name} en {col_type}: {e}, valeurs laissées en str.")

        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if row_count > 0:
            logging.info(f"Données déjà présentes dans {table_name}, passage du fichier CSV : {csv_file.name}")
        else:
            logging.info(f"Chargement des données dans {table_name} depuis {csv_file.name}")
            try:
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            except duckdb.Error as e:
                logging.error(f"Erreur lors du chargement de {csv_file.name}: {e}")

    def check_table(self, table_name, limit=10):
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

            df = self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit};").fetchdf()
            logging.info(f"{limit} lignes de '{table_name}':\n{df.to_string(index=False)}")

        except Exception as e:
            logging.error(f"Erreur lors de la lecture de la table '{table_name}': {e}")

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

        for csv_file in Path(self.csv_folder).glob("*.csv"):
            self.load_csv_file(csv_file)

        for csv_file in Path(self.csv_folder).glob("*.csv"):
            self.check_table(csv_file.stem)

    def close(self):
        """Ferme la connexion à la base de données."""
        self.conn.close()
        logging.info("Connexion à DuckDB fermée.")