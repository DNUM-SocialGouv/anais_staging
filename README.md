# anais_staging
Pipeline de l'étape de staging de la plateforme ANAIS

- CertDC


# Installation & Lancement du projet DBT

Cette section décrit les étapes nécessaires pour installer les dépendances, configurer DBT, instancier la base de données si besoin, et exécuter le projet.

---

## 1. Installation des dépendances via Poetry

Le projet utilise [Poetry](https://python-poetry.org/) pour la gestion des dépendances Python.  
Voici les étapes à suivre pour initialiser l’environnement :

```bash

# 2. Se placer dans le dossier du projet
cd chemin/vers/le/projet

# 3. Installer les dépendances
poetry install

# 4. Activer l’environnement virtuel Poetry
poetry shell
```

---

## 2. ⚙️ Configuration du fichier `profiles.yml`

DBT nécessite un fichier de configuration appelé `profiles.yml`, qui contient les informations de connexion à la base de données.

### Où se trouve le fichier ?

Il doit être placé dans le répertoire suivant :
- **Linux/macOS** : `~/.dbt/profiles.yml`
- **Windows** : `C:\Users\<VotreNom>\.dbt\profiles.yml`

> Si le dossier `.dbt` n’existe pas encore, vous pouvez le créer manuellement.  

### Où placer le fichier ?

Il doit être placé dans à la racine du projet Anais_staging (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_staging/profiles.yml`
- **Local** : `C:\Users\<VotreNom>\...\<projet>\profiles.yml`
 
Le fichier `profiles.yml` est disponible à la racine du repo.  


### Que contient le fichier ?

Il contient les informations relatives aux bases de données des différents projets :
- Staging (DuckDB et postegres)
- Helios (DuckDB et postegres)
- Matrice (DuckDB et postegres)
- InspectionControle (DuckDB et postegres)
- CertDC (DuckDB et postegres)

Seul le password des bases postgres n'est pas indiqué -> il est indiqué dans le `.env`

---

## 3. Lancement du pipeline :

L'ensemble de la Pipeline est exécuté depuis le `main.py`.

### Pour l'exécution de la pipeline:
1. Placer vous dans le bon répertoire `anais_staging`

```bash
cd anais_staging
```

2. Activer le `.venv`
```bash
source .venv/bin/activate
```

3. Lancer le `main.py`
```bash
python3 Staging/main.py --env "env" --profile "projet"
```
Avec env = 'local' ou 'anais' selon votre environnement de travail
et profile = 'Staging', 'Helios', 'Matrice', 'InspectionControle' ou 'CertDC' selon le projet que vous souhaitez lancer

### Pipeline sur env 'local':
1. Récupération des fichiers d'input. Ces fichiers sont placés manuellement dans le dossier `input` sous format **.csv** (les délimiteurs sont gérés automatiquement)
2. Création de la base DuckDB si inexistante.
3. Connexion à la base DuckDB
4. Création des tables si inexistantes
5. Lecture des csv avec standardisation des colonnes (sans caractères spéciaux) -> injection des données dans les tables
6. Vérification de l'injection
7. Exécution de la commande `run dbt` -> Création des vues relatives au projet
8. Export des vues dans le dossier `output`
9. Fermeture de la connexion à la base DuckDB

### Pipeline sur env 'anais':
1. Récupération des fichiers d'input. Ces fichiers sont récupérés automatiquement sur le SFTP et placés dans le dossier `input` sous format **.csv** (les délimiteurs sont gérés automatiquement)
2. Création de la base Postgres si inexistante.
3. Connexion à la base Postgres
4. Création des tables si inexistantes
5. Lecture des csv avec standardisation des colonnes (sans caractères spéciaux) -> injection des données dans les tables
6. Vérification de l'injection
7. Exécution de la commande `run dbt` -> Création des vues relatives au projet
8. Export des vues dans le dossier `output` au format **.csv**
9. Fermeture de la connexion à la base Postgres
10. Export des **.csv** en output vers le SFTP
