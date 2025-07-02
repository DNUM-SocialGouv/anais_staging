import os
import csv
import unicodedata
import pandas as pd
import logging
from io import StringIO

# === Configuration du logger ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/standardize_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def detect_delimiter(filepath, sample_size=4096):
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

def read_csv_resilient(filepath):
    """Tente de lire un CSV avec différents délimiteurs."""
    delimiters_to_try = [detect_delimiter(filepath), ";", ",", "¤"]
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
            return df, delimiter
        except pd.errors.ParserError as e:
            logging.warning(f"⚠️ Erreur de parsing avec '{delimiter}' pour {filepath} → {e}")
        except Exception as e:
            logging.warning(f"⚠️ Autre erreur avec '{delimiter}' → {e}")

    raise ValueError(f"❌ Impossible de lire le fichier CSV {filepath} avec les délimiteurs connus.")

def read_csv_special(filepath):
    """Lecture spécifique tolérante pour les fichiers avec délimiteur '¤' mal formés."""
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
        logging.info(f"✅ Lecture spéciale réussie avec délimiteur '¤' : {os.path.basename(filepath)}")
        return df, "¤"
    except Exception as e:
        raise ValueError(f"❌ Lecture spéciale échouée pour {filepath} → {e}")

def remove_accents(text):
    """Supprime les accents d’un texte."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def standardize_column_names(df):
    """Nettoie et standardise les noms de colonnes : minuscules, pas d'accents, caractères spéciaux en underscore."""
    df.columns = (
        df.columns
        .str.strip()
        .map(remove_accents)
        .str.lower()
        .str.replace(r"[^\w]", "_", regex=True)
        .str.replace(r"__+", "_", regex=True)
        .str.strip("_")
    )
    return df

def standardize_all_csv_columns(input_folder="input/"):
    """Standardise les noms de colonnes de tous les fichiers CSV en conservant le délimiteur d'origine."""
    for filename in os.listdir(input_folder):
        if not filename.endswith(".csv"):
            continue
        filepath = os.path.join(input_folder, filename)
        try:
            if filename == "sa_sivss.csv":
                df, delimiter = read_csv_special(filepath)
            else:
                df, delimiter = read_csv_resilient(filepath)

            df = standardize_column_names(df)
            df.to_csv(filepath, index=False, sep=delimiter, encoding="utf-8-sig")
            logging.info(f"✅ Colonnes standardisées pour : {filename} (délimiteur conservé : '{delimiter}')")

        except Exception as e:
            logging.error(f"❌ Erreur avec {filename} → {e}")

if __name__ == "__main__":
    standardize_all_csv_columns()