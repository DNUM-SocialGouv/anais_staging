# anais_staging
Pipeline de l'étape de staging de la plateforme ANAIS

- ref geo
- helios


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

### Où placer le fichier ?

Il doit être placé dans le répertoire suivant :
- **Linux/macOS** : `~/.dbt/profiles.yml`
- **Windows** : `C:\Users\<VotreNom>\.dbt\profiles.yml`

> Si le dossier `.dbt` n’existe pas encore, vous pouvez le créer manuellement.  

Le fichier `profiles.yml` est disponible à la racine du repo.  

---

## 3. 🏗️ Initialisation de la base de données (si nécessaire)

Si la base de données n’est pas encore instanciée, vous pouvez lancer le pipeline de création initiale.

### Lancement du pipeline :

```bash
poetry run python main.py
```

Ce script initialise la base de données et crée les tables nécessaires à l’exécution des modèles DBT.

---

## 4. ▶️ Lancement de DBT

Une fois les dépendances installées, la base de données prête et le fichier `profiles.yml` en place, vous pouvez exécuter les commandes DBT :

```bash
# Vérifie que tout est bien configuré avec la base de donnée
dbt debug

# Exécute les modèles
dbt run

```