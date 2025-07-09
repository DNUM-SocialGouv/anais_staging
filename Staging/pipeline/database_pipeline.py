# Packages
import duckdb
import os
from pathlib import Path
import logging
from typing import Callable
import re

# Modules
from pipeline.csv_management import export_to_csv

# Classe DataBasePipeline qui gère les actions relatives à n'importe quel database
class DataBasePipeline:
    def __init__(self,
                sql_folder: str = "Staging/output_sql/",
                csv_folder_input: str = "input/",
                csv_folder_output: str = "output/"):
        self.sql_folder = sql_folder
        self.csv_folder_input = csv_folder_input
        self.csv_folder_output = csv_folder_output

    def ensure_directories_exist(self):
        """ Crée les dossiers nécessaires s'ils n'existent pas. """
        for folder in [self.sql_folder, self.csv_folder_input, self.csv_folder_output]:
            os.makedirs(folder, exist_ok=True)
            logging.info(f"Dossier vérifié/créé : {folder}")
    
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
    
    def show_row_count(self, table: str):
        """
        Affiche le nombre de lignes d'une table.

        Parameters
        ----------
        table : str
            Nom de la table à vérifier (+ schema à indiquer si postgres).
        """
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if row_count == 0:
            logging.warning(f"⚠️ La table '{table}' est vide.")
            return True
        else:
            logging.info(f"✅ La table '{table}' contient {row_count} lignes.")
    
    def print_table(table: str, limit: int):
        """
        Affiche les premières lignes d'une table.

        Parameters
        ----------
        table : str
            Nom de la table à vérifier (+ schema à indiquer si postgres).
        limit : int, optional
            Nombre de lignes à afficher si print_table est True, 10 by default.
        """
        df = self.conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchdf()
        logging.info(f"🔍 Aperçu de '{table}' ({limit} lignes) :\n{df.to_string(index=False)}")

    def check_table(
        self,
        table_name: str,
        query_params: dict,
        print_table: bool = False,
        show_row_count: bool = False
    ) -> bool:
        """
        Vérifie l'existence et le contenu d'une table, avec option d'affichage.

        Parameters
        ----------
        table_name : str
            Nom de la table à vérifier.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        print_table : bool, optional
            Affiche les premières lignes de la table si elle existe, False by default.
        show_row_count : bool, optional
            Affiche le nombre total de lignes, False by default
        Returns
        -------
        bool
            True si la table existe (et non vide si applicable), False sinon.
        """
        try:
            # Vérifie si la table existe ou non
            table_exist = self.is_table_exist(query_params)

            if table_exist:
                # Vérifie si la table est remplie ou non
                if show_row_count:
                    self.show_row_count(table_name)

                # Affichage des premières valeurs d'une table
                if print_table:
                    self.print_table(table_name)
            return table_exist

        except Exception as e:
            logging.error(f"❌ Erreur lors de la vérification de la table '{table_name}' → {e}")
            return False

    def execute_sql_file(self, sql_file: Path, create_table_func: Callable[[str], None]):
        """
        Exécute un fichier SQL si la table n'existe pas. 

        Parameters
        ----------
        sql_file : Path
            Fichier SQL Create table.
        """
        sql = self._read_sql_file(sql_file)
        table_name = self.find_table_name_in_sql(sql)

        if not table_name:
            logging.info(f"❌ Nom de table introuvable dans le fichier SQL : '{sql_file.name}'")
            return
        else:
            query_params = {"schema": self.schema, "table": table_name}

            if not self.is_table_exist(query_params):
                try:
                    create_table_func(sql, query_params)
                    logging.info(f"✅ Table créée avec succès : {sql_file.name}")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de l'exécution du SQL {sql_file.name}: {e}")

    def export_csv(self, views_to_export, date):
        """
        Exporte les vues vers un format csv.

        Parameters
        ----------
        views_to_export : dict
            Liste des vues à exporter.
        date : str
            Date présente dans le nom des fichiers à exporter.
        """
        for table_name, csv_name in views_to_export.items():
            if table_name:
                export_to_csv(table_name, csv_name, self._fetch_df, self.csv_folder_output, date)
            else:
                logging.warning("⚠️ Aucune table spécifiée")

    def run(self):
        """
        Exécute toutes les étapes suivantes :
            - Création des tables SQL dans la base
            - Chargement des CSV et injection dans les tables
            - Vérification de la création
        """
        self.ensure_directories_exist()
        for sql_file in Path(self.sql_folder).glob("*.sql"):
            self.execute_sql_file(sql_file, self.create_table)

        logging.info(f"Début du chargement des fichiers CSV vers {self.typedb}.")
        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            self.load_csv_file(csv_file)
        logging.info(f"Fin du chargement des fichiers CSV vers {self.typedb}.")

        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            query_params = {"schema": self.schema, "table": csv_file.stem}
            self.check_table(csv_file.stem, query_params, print_table=False)