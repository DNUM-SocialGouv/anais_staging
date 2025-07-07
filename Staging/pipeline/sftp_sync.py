import os
import logging
import paramiko
import datetime
from dotenv import load_dotenv
from pipeline.csv_management import convert_excel_to_csv
from pipeline.loadfiles import load_colnames_YAML


class SFTPSync:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv("SFTP_HOST")
        self.port = int(os.getenv("SFTP_PORT"))
        self.username = os.getenv("SFTP_USERNAME")
        self.password = os.getenv("SFTP_PASSWORD")
        self.output_folder = os.getenv("SFTP_OUTPUT_FOLDER", "input/")

        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        logging.basicConfig(
            filename="logs/sftp_sync.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def connect(self):
        try:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info("Connexion SFTP établie.")
            return sftp, transport
        except Exception as e:
            logging.error(f"Erreur de connexion SFTP : {e}")
            raise

    def sftp_dir_exists(self, sftp, path):
        """ Vérifie l'existence d'un répertoire SFTP"""
        try:
            sftp.chdir(path)
            return True
        except IOError:
            return False

    def get_latest_file(self, sftp, remote_dir, keyword):
        try:
            files = sftp.listdir_attr(remote_dir)
            matching_files = [
                f for f in files
                if keyword in f.filename and not f.filename.endswith((".gpg", ".xlsx"))
            ]
            if 'DIAMANT' in remote_dir:
                matching_files = [
                    f for f in files
                    if keyword in f.filename and not f.filename.endswith((".gpg"))
                ]
            if not matching_files:
                logging.warning(f"Aucun fichier correspondant à '{keyword}' dans {remote_dir}")
                return None
            return max(matching_files, key=lambda f: f.st_mtime)
        except FileNotFoundError:
            logging.warning(f"Dossier introuvable : {remote_dir}")
            return None

    def download_file(self, sftp, remote_path, local_path):
        try:
            sftp.get(remote_path, local_path)
            logging.info(f"Téléchargé : {remote_path} → {local_path}")
        except Exception as e:
            logging.error(f"Échec du téléchargement {remote_path} : {e}")

    def download_all(self, files_list):
        sftp, transport = self.connect()
        files_to_download = [
            (item["path"], item["table"], item["file"])
            for item in files_list
        ]
        for remote_dir, keyword, local_filename in files_to_download:
            logging.info(f"Recherche du fichier contenant '{keyword}' dans {remote_dir}")
            latest_file = self.get_latest_file(sftp, remote_dir, keyword)
            
            if latest_file:
                remote_path = os.path.join(remote_dir, latest_file.filename)
                mod_time = datetime.datetime.fromtimestamp(latest_file.st_mtime)
                local_path = os.path.join(self.output_folder, local_filename)
                logging.info(f"Dernière version : {latest_file.filename} (modifié le {mod_time})")

                # Gestion des fichiers DIAMANT au format excel
                if '.xlsx' in latest_file.filename:
                    local_xlsx_path = local_path.replace('.csv', '.xlsx')
                    self.download_file(sftp, remote_path, local_xlsx_path)
                    convert_excel_to_csv(local_xlsx_path, local_path)
                # Autres fichiers au format csv
                else:
                    self.download_file(sftp, remote_path, local_path)
        sftp.close()
        transport.close()
        logging.info("Connexion SFTP fermée.")
        
        
    # def upload_all(self, local_folder="output/"):
    #     """Upload tous les fichiers CSV de output/ vers le SFTP dans les bons dossiers."""
    #     sftp, transport = self.connect()
    #     self.upload_mapping = PROJECT_PARAMS["base"]

    #     for filename in os.listdir(local_folder):
    #         if not filename.endswith(".csv"):
    #             continue

    #         local_path = os.path.join(local_folder, filename)
    #         remote_dir = self.upload_mapping.get(filename)

    #         if not remote_dir:
    #             logging.warning(f"❗ Fichier {filename} non mappé à un dossier distant SFTP, upload ignoré.")
    #             continue

    #         try:
    #             remote_path = os.path.join(remote_dir, filename)
    #             sftp.put(local_path, remote_path)
    #             logging.info(f"✅ Upload : {local_path} → {remote_path}")
    #         except Exception as e:
    #             logging.error(f"❌ Échec de l'upload {filename} → {e}")

    #     sftp.close()
    #     transport.close()
    #     logging.info("✅ Upload SFTP terminé et connexion fermée.")

    def upload_file_to_sftp(self, views_to_export: dict, output_dir: str, remote_dir: str, date: str):
        """Exporte en CSV les tables listées dans PG_EXPORT_TABLES du .env"""
        sftp, transport = self.connect()

        if not self.sftp_dir_exists(sftp, remote_dir):
            logging.error(f"❌ Répertoire SFTP inexistant : {remote_dir}")
            sftp.close()
            transport.close()
            return
        
        for _, csv_name in views_to_export.items():
            # Récupère le nom du fichier csv
            file_name = f'sa_{csv_name}_{date}.csv'
            local_path = os.path.join(output_dir, file_name)
            remote_path = os.path.join(remote_dir, file_name)

            # Vérifie l'existence du fichier csv dans output
            if os.path.exists(local_path):
                try:
                    sftp.put(local_path, remote_path)
                    logging.info(f"✅ Upload réussi: {file_name} → {remote_path}")
                except Exception as e:
                    logging.error(f"❌ Échec de l'upload {file_name} → {e}")
            else:
                logging.warning(f"⚠️ Fichier introuvable : {local_path}")
        sftp.close()
        transport.close()