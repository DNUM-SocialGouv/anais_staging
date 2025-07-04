import pandas as pd
import numpy as np
import csv
import os
import logging
from io import StringIO

# === Configuration du logging ===
logging.getLogger(__name__)

local_xlsx_path = './Staging/data/sa_insern_n_2_n_1.xlsx'
local_csv_path = './Staging/data/sa_insern_n_2_n_1.csv'


# === Fonctions principales ===
def fill(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fonction de remplissage des valeurs manquantes de la première colonne selon la méthode ffill.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe (TCD) sur lequel appliquer le remplissage.

    Returns
    -------
    pd.DataFrame
        Dataframe avec la première colonne remplie (sans valeur manquante).
    """
    # Remplir les valeurs manquantes dans la colonne "Colonne1"
    first_col = df.columns[0]
    df[first_col] = df[first_col].ffill()
    return df


def TCD_management(df: pd.DataFrame):
    for col_name in df.columns:
        # Si la colonne contient "Total général" -> suppression de la ligne
        df = df[df[col_name] != "Total général"]
        
        # Remplacement de "(vide)" par None
        mask = df[col_name] == "(vide)"
        df.loc[mask] = None
    return df


def convert_excel_to_csv(local_xlsx_path: str, local_csv_path: str):
    """
    Convertie un fichier Excel en csv (';').

    Parameters
    ----------
    local_xlsx_path : str
        Chemin local du fichier .xlsx.
    local_csv_path : str
        Chemin local du fichier .csv remplaçant le .xlsx.
    """
    # Conversion du fichier excel vers du csv
    df = pd.read_excel(local_xlsx_path, engine='openpyxl')
    df = fill(df)
    df = TCD_management(df)
    df.index.names = ['Column1']
    df.to_csv(local_csv_path, sep=";", quoting=csv.QUOTE_NONNUMERIC)

    # Suppression du fichier Excel
    try:
        os.remove(local_xlsx_path)
        logging.info(f"Fichier remplacé par un csv : {local_xlsx_path} -> {local_csv_path}")
    except Exception as e:
        logging.warning(f"⚠️ Erreur lors de la suppression de {local_xlsx_path} : {e}")

class ReadCsvWithDelimiter:
    def __init__(self, filepath, sample_size=4096):
        self.filepath = filepath
        self.sample_size = sample_size
        self.dialect = self.detect_delimiter()

    def detect_delimiter(self):
        """Détecte automatiquement le délimiteur du fichier CSV."""
        try:
            with open(self.filepath, 'r', encoding='utf-8-sig') as f:
                sample = f.read(self.sample_size)
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=";,¤")
                return dialect.delimiter
        except Exception as e:
            logging.warning(f"⚠️ Impossible de détecter le délimiteur pour {self.filepath} : {e} → ';' utilisé par défaut.")
            return ";"

    def read_csv_resilient(self):
        """Tente de lire un CSV avec différents délimiteurs."""
        delimiters_to_try = [self.dialect, ";", ",", "¤"]
        tried = set()

        for delimiter in delimiters_to_try:
            if delimiter in tried:
                continue
            tried.add(delimiter)

            try:
                df = pd.read_csv(
                    self.filepath,
                    delimiter=delimiter,
                    dtype=str,
                    quotechar='"',
                    encoding='utf-8-sig'
                )
                logging.info(f"✅ Lecture réussie avec le délimiteur '{delimiter}' pour {os.path.basename(self.filepath)}")
                return df
            except pd.errors.ParserError as e:
                logging.warning(f"⚠️ Erreur de parsing avec '{delimiter}' pour {self.filepath} → {e}")
            except Exception as e:
                logging.warning(f"⚠️ Autre erreur avec '{delimiter}' → {e}")

        raise ValueError(f"❌ Impossible de lire le fichier CSV {self.filepath} avec les délimiteurs connus.")

    def read_csv_with_custom_delimiter(self):
        try:
            with open(self.filepath, "rb") as f:
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
            
            logging.info(f"✅ Lecture réussie avec délimiteur '¤' après détection binaire : {os.path.basename(self.filepath)}")
            return df
        except Exception as e:
            logging.error(f"❌ Erreur lors de la lecture de {self.filepath} avec délimiteur '¤' → {e}")
            raise

    def read_csv_files(self)-> pd.DataFrame:
        try:
            if self.filepath.name == "sa_sivss.csv":
                return self.read_csv_with_custom_delimiter()
            else:
                return self.read_csv_resilient()
        except Exception as e:
            logging.error(f"❌ Lecture échouée pour {self.filepath.name} → {e}")
            return


def check_missing_columns(csv_file_name: str, df: pd.DataFrame, schema_df):
    table_columns = schema_df["column_name"].tolist()
    csv_columns = df.columns.tolist()

    missing_columns = set(table_columns) - set(csv_columns)
    extra_columns = set(csv_columns) - set(table_columns)

    if missing_columns:
        logging.warning(f"Colonnes manquantes dans {csv_file_name} : {missing_columns}")
        for col in missing_columns:
            df[col] = None

    if extra_columns:
        logging.warning(f"Colonnes en trop dans {csv_file_name} : {extra_columns}")
        df = df[table_columns]

def convert_columns_type(type_mapping: dict, df: pd.DataFrame, schema_df)-> pd.DataFrame:
    for _, row in schema_df.iterrows():
        col_name = row["column_name"]
        col_type = str(row["column_type"])

        if col_name in df.columns and col_type in type_mapping:
            try:
                if type_mapping[col_type] in ["int", "float"]:
                    df[col_name] = df[col_name].replace({None: 0, "": 0, pd.NA: 0, "nan": 0}).astype(float).astype(type_mapping[col_type])
                elif type_mapping[col_type] == "bool":
                    df[col_name] = df[col_name].replace({None: False, "": False, pd.NA: False}).astype(bool)
                elif type_mapping[col_type] == "datetime64":
                    df[col_name] = pd.to_datetime(df[col_name], format="%d-%m-%Y", errors="coerce")
                else:
                    df[col_name] = df[col_name].astype(type_mapping[col_type])
            except ValueError as e:
                logging.warning(f"Erreur de conversion de {col_name} en {col_type}: {e}, valeurs laissées en str.")
    return df



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



if __name__ == "__main__":
    convert_excel_to_csv(local_xlsx_path, local_csv_path)
