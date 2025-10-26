#!/usr/bin/env python3
"""
CSV Schema Validation Script for ANAIS Pipeline
Validates CSV files in input/staging/ directory against expected schemas from SQL files
"""

import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
import sys

# ANSI color codes for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

# Expected schemas (extracted from SQL CREATE TABLE statements)
SCHEMAS = {
    'sa_sirec': [
        'numero_de_la_reclamation', 'ancien_numero_de_la_reclamation', 'numero_demat_social',
        'service_gestionnaire', 'statut_de_la_reclamation', 'services_en_lecture',
        'signalement', 'date_de_la_demande_du_requerant', 'date_de_reception_a_l_ars',
        'service_de_premier_niveau', 'date_de_reception_au_service_de_premier_niveau',
        'description', 'date_de_creation_de_la_reclamation', 'domaine_fonctionnel',
        'mode_de_reception', 'prioritaire_oui_non', 'precisions_sur_le_caractere_prioritaire',
        'departement_de_la_reclamation', 'destinataire_s_de_la_reclamation',
        'destinataire_primaire', 'destinataire_secondaire', 'saisine_du_procureur_par_requerant',
        'institutions_de_provenance', 'date_de_reception_a_l_institution_de_provenance',
        'reponse_attendue', 'courrier_signale_oui_non', 'le_requerant_est',
        'le_requerant_est_anonyme', 'le_requerant_souhaite_garder_l_anonymat',
        'statut_du_requerant', 'plus_de_2_reclamations_deposees', 'usager_victime_non_identifiee',
        'sans_mis_en_cause', 'n_finess_rpps', 'autre_type', 'nom_structure', 'adresse',
        'code_postal', 'ville', 'service_pour_les_etablissements_sanitaires',
        'observations_du_mis_en_cause', 'motifs_igas_entree', 'motifs_igas_sortie',
        'niveau_de_competence_de_traitement_de_la_reclamation', 'institution_s_partenaire_s',
        'precisions_sur_le_niveau_de_competence_de_traitement_de_la_recl',
        'envoi_d_un_accuse_reception', 'date_d_envoi_de_l_ar', 'precisions_sur_le_non_envoi_de_l_ar',
        'date_de_transfert_a_l_institution_competente', 'date_de_prise_en_charge_par_le_service_gestionnaire',
        'type_de_traitement', 'precisions_sur_le_type_de_traitement',
        'reclamation_en_lien_avec_un_ou_plusieurs_signalements', 'numero_s_de_signalement_s_associe_s',
        'date_d_examen_en_commission', 'type_d_action', 'mesures_prises_par_le_mis_en_cause',
        'mesures_a_l_initiative_de', 'reponse_au_requerant_par_l_ars', 'date_de_reponse_au_requerant',
        'precisions_sur_la_reponse_au_requerant', 'date_de_reponse_a_l_institution_de_provenance',
        'motif_de_cloture', 'commentaire', 'date_de_cloture', 'siege_ars', 'motifs_declares'
    ],
    'sa_sivss': [
        'structure_intitule', 'numero_sivss', 'date_reception', 'famille_principale',
        'nature_principale', 'autre_signal_libelle', 'famille_secondaire', 'nature_secondaire',
        'autre_signal_secondaire_libelle', 'est_eigs', 'consequences_personne_exposee',
        'reclamation', 'declarant_est_anonyme', 'declarant_qualite_fonction',
        'declarant_categorie', 'declarant_organisme_type', 'declarant_etablissement_type',
        'declarant_organisme_numero_finess', 'declarant_organisme_nom',
        'declarant_organisme_region', 'declarant_organisme_departement',
        'declarant_organisme_code_postal', 'declarant_organisme_commune',
        'declarant_organisme_code_insee', 'survenue_cas_collectivite', 'scc_organisme_type',
        'scc_etablissement_type', 'scc_organisme_nom', 'scc_organisme_finess',
        'scc_organisme_region', 'scc_organisme_departement', 'scc_organisme_code_postal',
        'scc_organisme_commune', 'scc_organisme_code_insee', 'etat', 'support_signalement',
        'date_cloture', 'motif_cloture'
    ],
    'sa_siicea_decisions': [
        'type_de_decision', 'complement', 'theme_decision', 'sous_theme_decision',
        'nombre_d_ecart', 'commentaire', 'nombre_de_decisions', 'statut_de_decision',
        'identifiant_de_la_mission', 'date_realisation_visite_mission', 'type_de_mission',
        'modalite_d_investigation', 'groupe_de_cibles', 'nom_de_la_cible',
        'finess_geographique', 'finess_de_rattcahement', 'identifiant_rpps',
        'identifiant_siret', 'identifiant_uai', 'code_ape', 'type_d_etablissement',
        'departement', 'commune', 'coordonnateur_mission',
        'date_previsionelle', 'date_de_realisation', 'etat_d_avancement',
        'action_type_action', 'commentaire_action', 'date_echeance_previsionnelle_action',
        'date_realisation_action', 'etat_avancement_action'
    ],
    'sa_siicea_cibles': [
        'finess', 'finess_ej', 'rpps', 'siret', 'code_ape', 'code_uai',
        'groupe_cibles', 'nom_cible', 'code_departement', 'commune', 'adresse',
        'nb_mission_realisees', 'nb_mission_abandonnees', 'nb_decisions'
    ],
    'sa_siicea_missions_prog': [
        'identifiant_de_la_mission', 'mission_proposee_par', 'secteur_d_intervention',
        'code_theme_igas', 'theme_igas', 'code_theme_regional', 'theme_regional',
        'type_d_orientation', 'commanditaire_principal', 'referent_thematique',
        'references_reglementaires', 'type_de_mission', 'modalite_d_investigation',
        'type_de_planification', 'modalite_de_la_mission', 'critere_de_ciblage_1',
        'critere_de_ciblage_2', 'critere_de_ciblage_3', 'mission_conjointe_avec_1',
        'mission_conjointe_avec_2', 'code_uai', 'code_rpps', 'code_siret',
        'departement', 'commune', 'adresse', 'groupe_de_cibles', 'cible',
        'service_cible', 'caractere_juridique', 'type_de_cible', 'finess_geographique',
        'nom_agent_1', 'prenom_agent_1', 'role_mission_agent_1', 'profession_agent_1',
        'statut_agent_1', 'departement_agent_1', 'temps_total_agent_1',
        'nom_agent_2', 'prenom_agent_2', 'role_mission_agent_2', 'profession_agent_2',
        'statut_agent_2', 'departement_agent_2', 'temps_total_agent_2',
        'nom_agent_3', 'prenom_agent_3', 'role_mission_agent_3', 'profession_agent_3',
        'statut_agent_3', 'departement_agent_3', 'temps_total_agent_3',
        'nom_agent_4', 'prenom_agent_4', 'role_mission_agent_4', 'profession_agent_4',
        'statut_agent_4', 'departement_agent_4', 'temps_total_agent_4',
        'nom_agent_5', 'prenom_agent_5', 'role_mission_agent_5', 'profession_agent_5',
        'statut_agent_5', 'departement_agent_5', 'temps_total_agent_5',
        'nom_agent_6', 'prenom_agent_6', 'role_mission_agent_6', 'profession_agent_6',
        'statut_agent_6', 'departement_agent_6', 'temps_total_agent_6',
        'nom_agent_7', 'prenom_agent_7', 'role_mission_agent_7', 'profession_agent_7',
        'statut_agent_7', 'departement_agent_7', 'temps_total_agent_7',
        'nom_agent_8', 'prenom_agent_8', 'role_mission_agent_8', 'profession_agent_8',
        'statut_agent_8', 'departement_agent_8', 'temps_total_agent_8',
        'date_de_realisation', 'date_prevue', 'duree_totale_de_la_mission',
        'cout_total_de_la_mission', 'etat_d_avancement_de_la_mission', 'commentaire',
        'commentaire_1', 'commentaire_2'
    ],
    'sa_siicea_missions_real': [
        'identifiant_de_la_mission', 'mission_proposee_par', 'secteur_d_intervention',
        'code_theme_igas', 'theme_igas', 'code_theme_regional', 'theme_regional',
        'type_d_orientation', 'commanditaire_principal', 'referent_thematique',
        'references_reglementaires', 'type_de_mission', 'modalite_d_investigation',
        'type_de_planification', 'modalite_de_la_mission', 'critere_de_ciblage_1',
        'critere_de_ciblage_2', 'critere_de_ciblage_3', 'mission_conjointe_avec_1',
        'mission_conjointe_avec_2', 'code_uai', 'code_rpps', 'code_siret',
        'departement', 'commune', 'adresse', 'groupe_de_cibles', 'cible',
        'service_cible', 'caractere_juridique', 'type_de_cible', 'finess_geographique',
        'nom_agent_1', 'prenom_agent_1', 'role_mission_agent_1', 'profession_agent_1',
        'statut_agent_1', 'departement_agent_1', 'temps_total_agent_1',
        'nom_agent_2', 'prenom_agent_2', 'role_mission_agent_2', 'profession_agent_2',
        'statut_agent_2', 'departement_agent_2', 'temps_total_agent_2',
        'nom_agent_3', 'prenom_agent_3', 'role_mission_agent_3', 'profession_agent_3',
        'statut_agent_3', 'departement_agent_3', 'temps_total_agent_3',
        'nom_agent_4', 'prenom_agent_4', 'role_mission_agent_4', 'profession_agent_4',
        'statut_agent_4', 'departement_agent_4', 'temps_total_agent_4',
        'nom_agent_5', 'prenom_agent_5', 'role_mission_agent_5', 'profession_agent_5',
        'statut_agent_5', 'departement_agent_5', 'temps_total_agent_5',
        'nom_agent_6', 'prenom_agent_6', 'role_mission_agent_6', 'profession_agent_6',
        'statut_agent_6', 'departement_agent_6', 'temps_total_agent_6',
        'nom_agent_7', 'prenom_agent_7', 'role_mission_agent_7', 'profession_agent_7',
        'statut_agent_7', 'departement_agent_7', 'temps_total_agent_7',
        'nom_agent_8', 'prenom_agent_8', 'role_mission_agent_8', 'profession_agent_8',
        'statut_agent_8', 'departement_agent_8', 'temps_total_agent_8',
        'date_de_realisation', 'date_prevue', 'duree_totale_de_la_mission',
        'cout_total_de_la_mission', 'etat_d_avancement_de_la_mission', 'commentaire',
        'commentaire_1', 'commentaire_2'
    ]
}

# Expected delimiters by schema
DELIMITER_MAP = {
    'sa_sirec': ';',
    'sa_sivss': '¤',
    'sa_siicea_decisions': ',',
    'sa_siicea_cibles': ',',
    'sa_siicea_missions_prog': ',',
    'sa_siicea_missions_real': ','
}

def shorten_column_names(text: str, max_length: int = 63) -> str:
    """Truncate column names to PostgreSQL 63-character limit"""
    if len(text) > max_length:
        return text[:max_length]
    return text

def normalize_column_name(name: str) -> str:
    """
    Normalize column name to match pipeline standardization:
    - Convert to lowercase
    - Replace French accented characters
    - Replace special characters with underscores
    - Truncate to 63 characters (PostgreSQL limit)
    """
    # Replace French accented characters
    accent_map = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'à': 'a', 'â': 'a', 'ä': 'a',
        'î': 'i', 'ï': 'i',
        'ô': 'o', 'ö': 'o',
        'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c', 'ñ': 'n',
        'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
        'À': 'A', 'Â': 'A', 'Ä': 'A',
        'Î': 'I', 'Ï': 'I',
        'Ô': 'O', 'Ö': 'O',
        'Ù': 'U', 'Û': 'U', 'Ü': 'U',
        'Ç': 'C', 'Ñ': 'N'
    }

    for accented, plain in accent_map.items():
        name = name.replace(accented, plain)

    # Convert to lowercase
    col = name.lower()

    # Replace special characters (keep only alphanumeric and underscore)
    col = re.sub(r'[^\w]', '_', col)

    # Remove consecutive underscores
    col = re.sub(r'_+', '_', col)

    # Remove leading/trailing underscores
    col = col.strip('_')

    # Truncate to 63 characters
    return shorten_column_names(col)

def detect_delimiter(file_path: Path, potential_delimiters: List[str] = [',', ';', '¤', '\t']) -> str:
    """
    Detect CSV delimiter by reading first line and counting occurrences.
    """
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        # Skip any leading empty lines
        first_line = None
        for line in f:
            if line.strip():
                first_line = line
                break

        if not first_line:
            return ','  # Default fallback

        # Count each delimiter
        delimiter_counts = {}
        for delimiter in potential_delimiters:
            delimiter_counts[delimiter] = first_line.count(delimiter)

        # Return delimiter with highest count
        detected = max(delimiter_counts.items(), key=lambda x: x[1])
        return detected[0] if detected[1] > 0 else ','

def validate_csv_file(file_path: Path, schema_name: str) -> Dict:
    """
    Validate a single CSV file against its expected schema.

    Returns a dictionary with validation results.
    """
    result = {
        'file': file_path.name,
        'schema': schema_name,
        'valid': True,
        'issues': [],
        'csv_columns': [],
        'expected_columns': SCHEMAS[schema_name],
        'delimiter': None
    }

    try:
        # Use expected delimiter from mapping
        expected_delimiter = DELIMITER_MAP.get(schema_name, ',')
        result['delimiter'] = expected_delimiter

        # Read first line to get column names
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            # Skip warning line for SIREC (line 2 has warning text)
            lines = f.readlines()
            if schema_name == 'sa_sirec' and len(lines) > 1:
                header_line = lines[0]
            else:
                header_line = lines[0] if lines else ''

            # Parse CSV header
            csv_reader = csv.reader([header_line], delimiter=expected_delimiter)
            csv_columns_raw = next(csv_reader, [])

            # Normalize column names
            csv_columns = [normalize_column_name(col.strip()) for col in csv_columns_raw if col.strip()]
            result['csv_columns'] = csv_columns

            # Check column count
            expected_count = len(SCHEMAS[schema_name])
            actual_count = len(csv_columns)

            if actual_count != expected_count:
                result['valid'] = False
                result['issues'].append(
                    f"Column count mismatch: expected {expected_count}, got {actual_count}"
                )

            # Check for missing columns
            expected_set = set(SCHEMAS[schema_name])
            actual_set = set(csv_columns)

            missing = expected_set - actual_set
            extra = actual_set - expected_set

            if missing:
                result['valid'] = False
                result['issues'].append(f"Missing columns: {', '.join(sorted(missing))}")

            if extra:
                result['valid'] = False
                result['issues'].append(f"Extra columns: {', '.join(sorted(extra))}")

    except Exception as e:
        result['valid'] = False
        result['issues'].append(f"Error reading file: {str(e)}")

    return result

def print_result(result: Dict):
    """Print validation result in a readable format"""
    status = f"{Colors.GREEN}✓ VALID{Colors.END}" if result['valid'] else f"{Colors.RED}✗ INVALID{Colors.END}"

    print(f"{Colors.BOLD}{result['file']}{Colors.END} ({result['schema']})")
    print(f"  Status: {status}")
    print(f"  Delimiter: {result['delimiter']}")
    print(f"  Columns: {len(result['csv_columns'])} (expected: {len(result['expected_columns'])})")

    if result['issues']:
        print(f"  {Colors.RED}Issues:{Colors.END}")
        for issue in result['issues']:
            print(f"    - {issue}")

    print()

def main():
    """Main validation logic"""
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}CSV Schema Validation - input/staging/ Directory{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")

    # Get base path (tests directory)
    base_path = Path(__file__).parent.parent

    # Find all CSV files in input/staging directory
    csv_files = []
    staging_dir = base_path / 'input' / 'staging'

    if not staging_dir.exists():
        print(f"{Colors.RED}Staging directory not found: {staging_dir}{Colors.END}")
        print(f"{Colors.YELLOW}Expected location: DBT/anais_staging/input/staging/{Colors.END}")
        return 1

    # Map files to schemas based on file names (already renamed to sa_*.csv)
    file_schema_map = {
        'sa_sirec.csv': 'sa_sirec',
        'sa_sivss.csv': 'sa_sivss',
        'sa_siicea_decisions.csv': 'sa_siicea_decisions',
        'sa_siicea_cibles.csv': 'sa_siicea_cibles',
        'sa_siicea_missions_prog.csv': 'sa_siicea_missions_prog',
        'sa_siicea_missions_real.csv': 'sa_siicea_missions_real',
    }

    for csv_file in staging_dir.glob('*.csv'):
        # Find matching schema
        if csv_file.name in file_schema_map:
            schema_name = file_schema_map[csv_file.name]
            csv_files.append((csv_file, schema_name))

    if not csv_files:
        print(f"{Colors.YELLOW}No CSV files found in {staging_dir}{Colors.END}")
        print(f"{Colors.YELLOW}Expected files: sa_sirec.csv, sa_sivss.csv, sa_siicea_*.csv{Colors.END}")
        return 1

    print(f"Found {len(csv_files)} CSV file(s) to validate in {staging_dir}\n")

    # Validate each file
    results = []
    for file_path, schema_name in csv_files:
        result = validate_csv_file(file_path, schema_name)
        results.append(result)
        print_result(result)

    # Summary
    print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}Summary{Colors.END}")
    print(f"{Colors.BOLD}{'='*80}{Colors.END}")

    valid_count = sum(1 for r in results if r['valid'])
    invalid_count = len(results) - valid_count

    print(f"Total files: {len(results)}")
    print(f"{Colors.GREEN}Valid: {valid_count}{Colors.END}")
    if invalid_count > 0:
        print(f"{Colors.RED}Invalid: {invalid_count}{Colors.END}")
        return 1

    print(f"\n{Colors.GREEN}{Colors.BOLD}All files are valid!{Colors.END}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
