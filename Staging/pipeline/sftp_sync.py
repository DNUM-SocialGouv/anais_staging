import os
import logging
import paramiko
import datetime
from dotenv import load_dotenv

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

        self.files_to_download = [
            ("/SCN_BDD/INSERN", "DNUM_TdB_CertDc", f"{self.output_folder}sa_insern.csv"),
            ("/SCN_BDD/SIICEA", "GROUPECIBLES_SCN", f"{self.output_folder}sa_siicea_cibles.csv"),
            ("/SCN_BDD/SIICEA", "MISSIONSPREV_SCN", f"{self.output_folder}sa_siicea_missions.csv"),
            ("/SCN_BDD/SIICEA", "DECISIONS_SCN", f"{self.output_folder}sa_siicea_suites.csv"),
            ("/SCN_BDD/SIREC", "sirec", f"{self.output_folder}sa_sirec.csv"),
            ("/SCN_BDD/SIVSS", "SIVSS_SCN", f"{self.output_folder}sa_sivss.csv"),
            ("/SCN_BDD/T_FINESS", "t-finess", f"{self.output_folder}sa_t_finess.csv"),
            ("/SCN_BDD/INSEE", "v_comer", f"{self.output_folder}v_comer.csv"),
            ("/SCN_BDD/INSEE", "v_commune_comer", f"{self.output_folder}v_commune_comer.csv"),
            ("/SCN_BDD/INSEE", "v_commune_depuis", f"{self.output_folder}v_commune_depuis.csv"),
            ("/SCN_BDD/INSEE", "v_commune_2024", f"{self.output_folder}v_commune.csv"),
            ("/SCN_BDD/INSEE", "v_departement", f"{self.output_folder}v_departement.csv"),
            ("/SCN_BDD/INSEE", "v_region", f"{self.output_folder}v_region.csv"),
        ]

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

    def get_latest_file(self, sftp, remote_dir, keyword):
        try:
            files = sftp.listdir_attr(remote_dir)
            matching_files = [
                f for f in files
                if keyword in f.filename and not f.filename.endswith((".gpg", ".xlsx"))
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

    def download_all(self):
        sftp, transport = self.connect()
        for remote_dir, keyword, local_filename in self.files_to_download:
            logging.info(f"Recherche du fichier contenant '{keyword}' dans {remote_dir}")
            latest_file = self.get_latest_file(sftp, remote_dir, keyword)
            if latest_file:
                remote_path = os.path.join(remote_dir, latest_file.filename)
                mod_time = datetime.datetime.fromtimestamp(latest_file.st_mtime)
                logging.info(f"Fichier trouvé : {latest_file.filename} (modifié le {mod_time})")
                self.download_file(sftp, remote_path, local_filename)
        sftp.close()
        transport.close()
        logging.info("Connexion SFTP fermée.")
        
        
    def upload_all(self, local_folder="output/"):
        """Upload tous les fichiers CSV de output/ vers le SFTP dans les bons dossiers."""
        sftp, transport = self.connect()

        upload_mapping = {
            "sa_insern.csv": "/SCN_BDD/INSERN",
            "sa_siicea_cibles.csv": "/SCN_BDD/SIICEA",
            "sa_siicea_missions.csv": "/SCN_BDD/SIICEA",
            "sa_siicea_suites.csv": "/SCN_BDD/SIICEA",
            "sa_sirec.csv": "/SCN_BDD/SIREC",
            "sa_sivss.csv": "/SCN_BDD/SIVSS",
            "sa_t_finess.csv": "/SCN_BDD/T_FINESS",
            "v_comer.csv": "/SCN_BDD/INSEE",
            "v_commune.csv": "/SCN_BDD/INSEE",
            "v_commune_comer.csv": "/SCN_BDD/INSEE",
            "v_commune_depuis.csv": "/SCN_BDD/INSEE",
            "v_departement.csv": "/SCN_BDD/INSEE",
            "v_region.csv": "/SCN_BDD/INSEE",
        }

        for filename in os.listdir(local_folder):
            if not filename.endswith(".csv"):
                continue

            local_path = os.path.join(local_folder, filename)
            remote_dir = upload_mapping.get(filename)

            if not remote_dir:
                logging.warning(f"❗ Fichier {filename} non mappé à un dossier distant SFTP, upload ignoré.")
                continue

            try:
                remote_path = os.path.join(remote_dir, filename)
                sftp.put(local_path, remote_path)
                logging.info(f"✅ Upload : {local_path} → {remote_path}")
            except Exception as e:
                logging.error(f"❌ Échec de l'upload {filename} → {e}")

        sftp.close()
        transport.close()
        logging.info("✅ Upload SFTP terminé et connexion fermée.")