import pandas as pd
import numpy as np
import csv
import os
import logging

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


def anomalies_management(df: pd.DataFrame):
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
    df = anomalies_management(df)
    df.index.names = ['Column1']
    df.to_csv(local_csv_path, sep=";", quoting=csv.QUOTE_NONNUMERIC)

    # Suppression du fichier Excel
    try:
        os.remove(local_xlsx_path)
        logging.info(f"Fichier remplacé par un csv : {local_xlsx_path} -> {local_csv_path}")
    except Exception as e:
        logging.warning(f"⚠️ Erreur lors de la suppression de {local_xlsx_path} : {e}")


if __name__ == "__main__":
    convert_excel_to_csv(local_xlsx_path, local_csv_path)
