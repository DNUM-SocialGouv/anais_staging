
import os
import csv
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
from io import StringIO

# Chargement des variables d’environnement
load_dotenv()

# Configuration du logger PostgreSQL
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/postgres_loader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class PostgreSQLLoader:
    def __init__(self, input_folder="input/", export_folder="output/"):
        self.input_folder = input_folder
        self.export_folder = export_folder
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.user = os.getenv("PG_USER")
        self.password = os.getenv("PG_PASSWORD")
        self.database = os.getenv("PG_DATABASE")
        self.schema = os.getenv("PG_SCHEMA", "public")
        self.engine = self.init_engine()
        os.makedirs(export_folder, exist_ok=True)

    def init_engine(self):
        try:
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(url)
            logging.info("Connexion PostgreSQL établie avec succès.")
            return engine
        except Exception as e:
            logging.error(f"Erreur de connexion PostgreSQL : {e}")
            raise

    def execute_create_sql_files(self, sql_folder="output_sql_postgres/"):
        logging.info(f"Exécution des scripts SQL dans {sql_folder}")
        for sql_file in Path(sql_folder).glob("*.sql"):
            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    sql = f.read()
                with self.engine.begin() as connection:
                    connection.execute(text(sql))
                logging.info(f"✅ Script exécuté avec succès : {sql_file.name}")
            except Exception as e:
                logging.error(f"❌ Erreur d'exécution {sql_file.name} : {e}")

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

    def load_all_csv_from_input(self):
        logging.info("Début du chargement des fichiers CSV vers PostgreSQL.")
        for file in os.listdir(self.input_folder):
            if not file.endswith(".csv"):
                continue

            table_name = os.path.splitext(file)[0]
            csv_path = os.path.join(self.input_folder, file)
            logging.info(f"📥 Chargement du fichier : {file}")

            try:
                if file == "sa_sivss.csv":
                    df = self.read_csv_with_custom_delimiter(csv_path)
                else:
                    df = self.read_csv_resilient(csv_path)

                df.columns = df.columns.str.strip().str.replace(r"[^\w]", "_", regex=True)

                with self.engine.begin() as connection:
                    connection.execute(text(f'SET search_path TO {self.schema}'))

                    # Vérifie si la table existe
                    table_exists = connection.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = :schema 
                            AND table_name = :table
                        )
                    """), {"schema": self.schema, "table": table_name}).scalar()

                    if not table_exists:
                        logging.info(f"🔧 Table '{table_name}' absente → création avec if_exists='replace'")
                        df.to_sql(
                            table_name,
                            connection,
                            if_exists="replace",
                            index=False,
                            method='multi',
                            chunksize=1000
                        )
                    else:
                        logging.info(f"♻️ Table '{table_name}' existante → suppression + insertion")
                        
                        try:
                            connection.execute(text(f"DELETE FROM {table_name}"))
                        except Exception as e:
                            logging.warning(f"⚠️ Erreur lors du DELETE de {table_name} : {e} → remplacement total")
                            df.to_sql(
                                table_name,
                                connection,
                                if_exists="replace",
                                index=False,
                                method='multi',
                                chunksize=1000
                            )
                        else:
                            df.to_sql(
                                table_name,
                                connection,
                                if_exists="append",
                                index=False,
                                method='multi',
                                chunksize=1000
                            )

                logging.info(f"✅ Table '{table_name}' remplacée avec succès ({file})")

            except Exception as e:
                logging.error(f"❌ Erreur pour le fichier {file} → {e}")

        logging.info("✅ Chargement PostgreSQL terminé.")

    def export_tables_from_env(self, output_folder="output_export/"):
        """Exporte en CSV les tables listées dans PG_EXPORT_TABLES du .env"""
        tables_str = os.getenv("PG_EXPORT_TABLES", "")
        if not tables_str:
            logging.warning("⚠️ Aucune table spécifiée dans PG_EXPORT_TABLES")
            return

        os.makedirs(output_folder, exist_ok=True)

        tables = [t.strip() for t in tables_str.split(",") if t.strip()]
        for table in tables:
            try:
                output_path = os.path.join(output_folder, f"{table}.csv")
                logging.info(f"📤 Export de la table '{table}' vers {output_path}")
                with self.engine.begin() as connection:
                    connection.execute(text(f'SET search_path TO {self.schema}'))
                    df = pd.read_sql_table(table, connection)
                    df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
                logging.info(f"✅ Table '{table}' exportée avec succès.")
            except Exception as e:
                logging.error(f"❌ Erreur lors de l'export de '{table}' → {e}")