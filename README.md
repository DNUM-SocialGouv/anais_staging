# Anais Staging
Pipeline de l'étape de Staging de la plateforme ANAIS

# Installation & Lancement du projet DBT

Cette section décrit les étapes nécessaires pour installer les dépendances, configurer DBT, instancier la base de données si besoin, et exécuter le projet.

---

## 1. Installation des dépendances via UV

Le projet utilise [UV] pour la gestion des dépendances Python.  
Voici les étapes à suivre pour initialiser l’environnement :

```bash

# 1. Se placer dans le dossier du projet
cd chemin/vers/le/projet

# 2. Vérifier que uv est installé
uv --version
pip install uv # Si pas installé

# 3. Installer les dépendances
uv sync
```

---
## 2. ⚙️Configuration du projet

Plusieurs fichiers doivent être correctement configurés et placés dans le bon répertoire pour le bon fonctionnement de la pipeline.

### 2.1 Fichier `profiles.yml`

DBT nécessite un fichier de configuration appelé `profiles.yml`, qui contient les informations de connexion à la base de données.

#### Où se trouve le fichier ?

Il doit se trouver dans le répertoire suivant :
- **Linux/macOS** : `~/.dbt/profiles.yml`
- **Windows** : `C:\Users\<NomUtilisateur>\.dbt\profiles.yml`

> Si le dossier `.dbt` n’existe pas encore, vous pouvez le créer manuellement.  

#### Où placer le fichier ?

Il doit être placé dans à la racine du projet Anais_staging (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_staging/profiles.yml`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\profiles.yml`
 
Le fichier `profiles.yml` est disponible à la racine du repo.  


#### Que contient le fichier ?

Il contient les informations relatives aux bases de données des différents projets :
- Staging (DuckDB et postgres)
- Helios (DuckDB et postgres)
- Matrice (DuckDB et postgres)
- InspectionControlePA et InspectionControlePH (DuckDB et postgres)
- CertDC (DuckDB et postgres)

Exemple :
```yaml
Staging:
  target: local
  outputs:
    anais:
      type: postgres
      host: xx.xx.xx.xx
      user: <Nom_utilisateur>
      password: "{{ env_var('STAGING_PASSWORD') }}"
      port: xxxx
      schema: public
      dbname: staging
    local:
      type: duckdb
      path: data/staging/duckdb_database.duckdb
      schema: main
```

Seul le mot de passe des bases postgres n'est pas indiqué dans le fichier profiles.yml -> il est indiqué dans le `.env`

### 2.2 Fichier `.env`

Ce fichier `.env` contient les mots de passe d'accès aux bases postgres et au SFTP.
Il est secret, donc indisponible sur le git.
Il doit donc être créé manuellement.

#### Où placer le fichier ?

Il doit être placé dans à la racine du projet Anais_staging (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_staging/.env`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\.env`
 
Le fichier `.env` est disponible à la racine du repo.

#### Que contient le fichier ?

Il contient les variables suivantes, avec leurs valeurs entre guillement `" "` :

- `SFTP_HOST = "<host du SFTP>"`
- `SFTP_PORT = <port du SFTP>`
- `SFTP_USERNAME = "<nom de l'utilisateur>"`
- `SFTP_PASSWORD = "<Mot de passe du SFTP>"`
- `STAGING_PASSWORD = "<Mot de passe de la base staging>"`
- `HELIOS_PASSWORD = "<Mot de passe de la base helios>"`
- `INSPECTION_CONTROLE_ADMIN_PA_PASSWORD = "<Mot de passe de la base inspection_controle>"`
- `INSPECTION_CONTROLE_ADMIN_PH_PASSWORD = "<Mot de passe de la base inspection_controle>"`
- `MATRICE_PA_PASSWORD = "<Mot de passe de la base matrice>"`
- `MATRICE_PH_PASSWORD = "<Mot de passe de la base matrice>"`
- `CERTDC_PASSWORD = "<Mot de passe de la base certelec_dc>"`


### 2.3 ⚙️ Fichier `metadata.yml`

Le fichier `metadata.yml` contient le paramétrage relatif aux fichiers en entrée et en sortie et aux répertoires du projet.

#### Section *directory*

Contient les répertoires du projet :
```yaml
  local_directory_input: "input/<Nom_projet>/"
  local_directory_output: "output/<Nom_projet>/"
  models_directory: "<dbtNom_projet>"
  create_table_directory: "output_sql/<Nom_projet>/"
  remote_directory_input: "/SCN_BDD/<Nom_projet>/input"
  remote_directory_output: "/SCN_BDD/<Nom_projet>/output"
```

Avec :
- `local_directory_input` = répertoire en local où sont trouvables les fichiers csv en entrée.
- `local_directory_output` = répertoire en local où sont enregistrés les fichiers csv en sortie.
- `models_directory` = répertoire dans lequel sont enregistrés les modèles dbt du projet.
- `create_table_directory` = répertoire où sont enregistrés les fichiers SQL Create table.
- `remote_directory_input` = répertoire SFTP où sont enregistrés les fichiers csv des tables d'origine en sortie. Ce répertoire existe pour faciliter la recette. Le projet Staging ne nécessite pas de valeur -> `null`
- `remote_directory_output` = répertoire SFTP où sont enregistrés les fichiers csv en sortie. Le projet Staging ne nécessite pas de valeur -> `null`

#### Section *files_to_download*

Contient les informations relatives aux fichiers csv provenant du SFTP.
La section `files_to_download` (fichier à récupérer) contient :

``` yaml
  files_to_download:
    - path: "/SCN_BDD/INSERN"
      keyword: "DNUM_TdB_CertDc"
      file: "sa_insern.csv"
```

Avec :
- `path` = Chemin du fichier sur le SFTP.
- `keyword` = Terme dans le nom du fichier qui permet de le distinguer des autres fichiers. Le fichier le plus récent contenant ce terme sera récupéré.
- `file` = Nom d'enregistrement du fichier une fois importé.

Section inutilisée en local.


#### Section *table_to_copy*, *input_to_download* et *files_to_upload*

La section `table_to_copy` indique les tables de staging à copier dans la base du projet.
La section `input_to_download` indique les tables à envoyer en csv dans le remote_directory_input. Nécessaire pour la recette.
La section `files_to_upload` indique les vues à envoyer en csv dans le remote_directory_output.

```yaml
  table_to_copy:
    <Nom_de_la_vue_sql>: <nom_de_la_table>
    ...
    <Nom_de_la_vue_sql>: <nom_de_la_table>
  input_to_download:
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
    ...
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
  files_to_upload:
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
    ...
    <Nom_de_la_vue_sql>: <radical_du_fichier_csv_exporté>
```

Avec :
- `<Nom_de_la_vue_sql>`: le nom de la vue SQL dans la base de données. Aussi le nom du modèle déployé
- `<radical_du_fichier_csv_exporté>`: radical du nom du csv à exporter. La date du jour d'exécution sera ajouté automatiquement au nom du fichier exporté : `'<radical_du_fichier_csv_exporté>_<date_du_jour>.csv'`. On peut également préciser le répertoire de destination dans le nom.

### 2.4 Fichier `sources.yml`

Ce fichier `sources.yml` contient la liste des des tables sources nécessaires pour lancer les modèles dbt, pour local et pour anais. 

#### Où placer le fichier ?

Il doit être placé au même endroit que les modèles dbt du projet (au même niveau que le README et pyproject.toml) :
- **VM Cegedim** : `~/anais_staging/<dbtNom_Projet>/models/sources.yml`
- **Local** : `C:\Users\<NomUtilisateur>\...\<projet>\<dbtNom_Projet>\models\sources.yml`

#### Que contient le fichier ?

Il contient la liste des tables sources nécessaires (celles créées via CREATE TABLE) pour le lancement des modèles dbt.
La liste est dupliquée pour les deux sources : local (public) et anais (main).

```yaml
sources:
  - name: main
    tables:
      - name: sa_insern
      ...
      - name: sa_tdb_esms
  - name: public
    tables:
      - name: sa_insern
      ...
      - name: sa_tdb_esms
```
Avec main ou public respectivement le nom des schémas des bases postgres et duckDB. Nom indiqué dans `profiles.yml`.

---
## 3. Lancement du pipeline :

La Pipeline exécutée est celle du package `anais_pipeline` dans la branche du même nom du repo anais_staging. Elle est importée comme un package dans le `pyproject.toml`. L'ensemble de la Pipeline est exécutée depuis le `main.py`

### 3.1 Exécution de la pipeline pour Staging:

```bash
# Se placer dans anais_staging
cd anais_staging

#  Lancer le `main.py`
uv run -m pipeline.main --env "local" --profile "Staging"
```
Avec env = 'local' ou 'anais' selon votre environnement de travail
et profile = 'Staging'

#### Pipeline Staging sur env 'local':
1. Récupération des fichiers d'input. Ces fichiers doivent être placés manuellement dans le dossier `input/` sous format **.csv** (les délimiteurs sont gérés automatiquement).
2. Création de la base DuckDB si inexistante.
3. Connexion à la base DuckDB.
4. Création des tables, même si déjà existantes. Les fichiers sql de création de table (CREATE TABLE) doivent être placés dans le répertoire indiqué dans le `create_table_directory` de `metadata.yml`.
5. Lecture des csv avec standardisation des colonnes (ni caractères spéciaux, ni majuscule) -> injection des données dans les tables.
6. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
7. Vérification de la réussite de l'injection.
8. Fermeture de la connexion à la base DuckDB.
9. Exécution de la commande `run dbt` -> Création des vues relatives au projet.


#### Pipeline Staging sur env 'anais':
1. Récupération des fichiers d'input depuis le SFTP. Ces fichiers sont placés dans le dossier `input/` sous format **.csv** (les délimiteurs sont gérés automatiquement).
2. Connexion à la base Postgres.
3. Création des tables, même si déjà existantes. Les fichiers sql de création de table (CREATE TABLE) doivent être placés dans le répertoire indiqué dans le `create_table_directory` de `metadata.yml`.
4. Lecture des csv avec standardisation des colonnes (ni caractères spéciaux, ni majuscule) -> injection des données dans les tables.
5. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
6. Vérification de la réussite de l'injection.
7. Fermeture de la connexion à la base Postgres.
8. Exécution de la commande `run dbt` -> Création des vues relatives au projet.

### 3.2 Exécution de parties de la pipeline
#### Importation seule des fichiers depuis le SFTP

Pour seulement importer les fichiers .csv du SFTP vers le répertoire local :

```bash
uv run -m pipeline.utils.sftp_sync --env "local" --profile "Staging"

```

#### Exécution du dbt run

Pour seulement exécuter le dbt run afin de tester le fonctionnement des modèles :

```bash
uv run -m pipeline.utils.dbt_tools --env "local" --profile "Staging"
```

# Documentation et architecture du projet
## 1. Déployement de la documentation
En cours

## 2. Architecture du projet

```plaintext
.
├── data
│   └── staging
│       └── duckdb_database.duckdb
├── dbtStaging
│   ├── analyses
│   ├── dbt_packages
│   ├── dbt_project.yml
│   ├── logs
│   │   └── dbt.log
│   ├── macros
│   │   ├── date_management.sql
│   │   ├── get_source_schema.sql
│   │   ├── iif_replacement.sql
│   │   ├── row_count.sql
│   │   └── split_string_replacement.sql
│   ├── models
│   │   └── staging
│   │       ├── base
│   │       │   ├── staging__sa_insern.sql
│   │       │   ├── staging__sa_siicea_cibles.sql
│   │       │   ├── staging__sa_siicea_decisions.sql
│   │       │   ├── staging__sa_siicea_missions.sql
│   │       │   ├── staging__sa_sirec.sql
│   │       │   ├── staging__sa_sivss.sql
│   │       │   ├── staging__sa_t_finess.sql
│   │       │   ├── staging__v_comer.sql
│   │       │   ├── staging__v_commune_comer.sql
│   │       │   ├── staging__v_commune_depuis.sql
│   │       │   ├── staging__v_commune.sql
│   │       │   ├── staging__v_departement.sql
│   │       │   └── staging__v_region.sql
│   │       ├── certdc
│   │       │   ├── staging__cert_dc_insern_2023_2024.sql
│   │       │   ├── staging__cert_dc_insern_n2_n1.sql
│   │       │   ├── staging__dc_det.sql
│   │       │   ├── staging__sa_esms.sql
│   │       │   ├── staging__sa_hubee.sql
│   │       │   ├── staging__sa_insee_histo.sql
│   │       │   ├── staging__sa_pmsi.sql
│   │       │   ├── staging__sa_rpu.sql
│   │       │   └── staging__sa_usld.sql
│   │       ├── helios
│   │       │   ├── staging__helios_siicea_missions.sql
│   │       │   ├── staging__helios_sirec.sql
│   │       │   └── staging__helios_sivss.sql
│   │       ├── inspection_controle_PA
│   │       │   ├── staging__sa_siicea_missions_prog.sql
│   │       │   └── staging__sa_siicea_missions_real.sql
│   │       ├── matrice
│   │       │   ├── staging__matrice_ciblage.sql
│   │       │   ├── staging__matrice_siicea_cibles.sql
│   │       │   ├── staging__matrice_siicea_missions_real.sql
│   │       │   ├── staging__matrice_sirec.sql
│   │       │   └── staging__matrice_tdb_esms.sql
│   │       ├── ref_geo
│   │       │   ├── staging__ref_communes.sql
│   │       │   ├── staging__ref_departements.sql
│   │       │   ├── staging__ref_geo.sql
│   │       │   └── staging__ref_regions.sql
│   │       ├── siicea
│   │       │   └── staging__siicea_input_test.sql
│   │       ├── sirec
│   │       │   └── staging__sirec_input_test.sql
│   │       ├── sources.yml
│   │       ├── staging__cert_dc_finess.sql
│   │       ├── staging__cert_dc_insern.sql
│   │       ├── staging__dgcs_query.sql
│   │       └── tdb
│   │           ├── staging__tdb_ic_finess_500.sql
│   │           ├── staging__tdb_ic_siicea_cibles.sql
│   │           ├── staging__tdb_ic_siicea_missions_prog.sql
│   │           └── staging__tdb_ic_siicea_missions_real.sql
│   ├── README.md
│   ├── seeds
│   ├── snapshots
│   ├── staging__helios_siicea_decisions.sql
│   ├── staging__helios_siicea_missions_test.sql
│   ├── staging__matrice_siicea_missions_prog.sql
│   ├── staging__sa_siicea_missions_prog.sql
│   └── tests
├── input
│   └── staging
├── logs
│   ├── dbt.log
│   ├── log_anais.log
│   └── log_local.log
├── output
│   └── staging
├── output_sql
│   │   └── staging
│   │       ├── cert_dc_insern_2023_2024.sql
│   │       ├── cert_dc_insern_n2_n1.sql
│   │       ├── dc_det.sql
│   │       ├── sa_esms.sql
│   │       ├── sa_hubee.sql
│   │       ├── sa_insee_histo.sql
│   │       ├── sa_insern.sql
│   │       ├── sa_pmsi.sql
│   │       ├── sa_rpu.sql
│   │       ├── sa_siicea_cibles.sql
│   │       ├── sa_siicea_decisions.sql
│   │       ├── sa_siicea_missions_prog.sql
│   │       ├── sa_siicea_missions_real.sql
│   │       ├── sa_siicea_missions.sql
│   │       ├── sa_sirec.sql
│   │       ├── sa_sivss.sql
│   │       ├── sa_t_finess.sql
│   │       ├── sa_usld.sql
│   │       ├── v_comer.sql
│   │       ├── v_commune_comer.sql
│   │       ├── v_commune_depuis.sql
│   │       ├── v_commune.sql
│   │       ├── v_departement.sql
│   │       └── v_region.sql
├── .env
├── metadata.yml
├── poetry.lock
├── profiles.yml
├── pyproject.toml
└── README.md
```

## 3. Utilités des fichiers
### 3.1 Fichiers à la racine `./`
Répertoire d'orchestration de la pipeline Python.

- `.env `: Fichier secret contenant le paramétrage vers le SFTP et les mots de passe des bases de données postgres.
- `metadata.yml` : Contient les configurations du projets et la liste des fichiers .csv provenant du SFTP.
- `output_sql/` : Répertoire qui contient les fichiers .sql de création de table (CREATE TABLE).
- `logs/` : Répertoire des logs local et anais.
- `data/` : Répertoire des bases de données DuckDB.
- `input/` : Répertoire de stockage des fichiers .csv en entrée.
- `output/` : Répertoire de stockage des fichiers .csv en sortie.
- `profiles.yml` : Contient les informations relatives aux bases des différents projets.
- `pyproject.toml` : Fichier contenant les dépendances et packages nécessaires pour le lancement de la pipeline.

### 3.2 Fichiers dans dbtStaging `./dbtStaging/`
Répertoire de fonctionnement des modèles DBT -> création de vues SQL.

- `dbt_project.yml` : Fichier de configuration de DBT (obligatoire).
- `macros/` : Répertoire de stockage des macros jinja.
- `models/` : Répertoire de stockage des modèles dbt.
- `models/staging/sources.yml` : Fichier contenant le nom des tables sources nécessaires pour le lancement des modèles .

