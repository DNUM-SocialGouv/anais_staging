# --- Importation de fichier de configuration ou de correspondance
# Importation des packages
import os
from pathlib import Path
import yaml
import logging


# Fonction
def load_YAML(file_name: str, config_file_dir: str = None) -> dict:
    """
    Charge un fichier YAML.

    Parameters
    ----------
    file_name : str
        Nom du fichier de configuration YAML.
    config_file_dir : str
        Chemin du fichier, by default None.

    Returns
    -------
    dict
        La configuration pour l'environnement spécifié.

    Raises
    -------
    FileNotFoundError
        Si le fichier de configuration est introuvable.
    yaml.YAMLError
        Si le fichier YAML est mal formaté.  
    """
    if config_file_dir:
        path = os.path.join(config_file_dir, file_name)
    else:
        current_dir = Path(__file__).resolve().parent
        path = current_dir.parent / "pipeline" / file_name
    try:
        with open(path, 'r') as f:
            file = yaml.safe_load(f)
            logging.info(f"Acces config file readed from {path}")
            return file

    except FileNotFoundError:
        logging.error(f"Fichier de configuration introuvable {path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Erreur de parsing YAML dans le fichier {file_name} : {e}")
        raise
    except Exception as e:
        logging.error(f"Erreur inattendue lors du chargement du fichier de configuration : {e}")
        raise


def load_colnames_YAML(file_name: str, table: str, config_file_dir: str = None) -> dict:
    """
    Charge le fichier de configuration et récupère la liste des colonnes d'une table donnée.

    Parameters
    ----------
    file_name : str
        Nom du fichier de configuration YAML.
    table : str
        Nom de la table dont on veut récupérer les colonnes.
    config_file_dir : str
        Chemin du fichier, by default None.

    Returns
    -------
    dict | None
        Dictionnaire contenant les colonnes à garder et à renommer, ou None en cas d'erreur.

    Raises
    -------
    FileNotFoundError
        Si le fichier de configuration est introuvable.
    KeyError
        Si la table ou la liste des colonnes n'est pas présente dans le fichier.
    yaml.YAMLError
        Si le fichier YAML est mal formaté.
    """
    try:
        metadata = load_YAML(file_name, config_file_dir)

        if table not in metadata:
            raise KeyError(f"La table '{table}' n'existe pas dans le fichier {file_name}.")
        
        return metadata[table]
    except KeyError as e:
        logging.error(e)
        raise
    except Exception as e:
        logging.error(f"Erreur inattendue lors du chargement de la configuration : {e}")
        raise