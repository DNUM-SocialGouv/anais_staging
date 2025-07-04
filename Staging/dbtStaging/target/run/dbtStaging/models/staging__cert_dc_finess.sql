
  create view "staging"."public"."staging__cert_dc_finess__dbt_tmp"
    
    
  as (
    

WITH finess AS (
    SELECT 
        finess,
        finess8,
        etat,
        date_extract_finess,
        rs,
        type,
        ej_finess,
        ej_rs,
        et_finess,
        et_rs,
        siren,
        siret,
        date_autorisation,
        date_ouverture,
        date_maj_finess,
        adresse_num_voie,
        adresse_comp_voie,
        adresse_type_voie,
        adresse_nom_voie,
        adresse_lieuditbp,
        adresse_code_postal,
        adresse_lib_routage,
        telephone,
        telecopie,
        CASE 
            WHEN LENGTH(com_code) = 4 THEN '0' || com_code 
            ELSE com_code 
        END AS com_code,        statut_jur_code,
        statut_jur_lib,
        statut_jur_etat,
        statut_jur_niv3_code,
        statut_jur_niv3_lib,
        statut_jur_niv2_code,
        statut_jur_niv2_lib,
        statut_jur_niv1_code,
        statut_jur_niv1_lib,
        CAST(categ_code AS INTEGER) AS categ_code,
        categ_lib,
        categ_lib_court,
        categ_etat,
        categ_niv3_code,
        categ_niv3_lib,
        categ_niv2_code,
        categ_niv2_lib,
        categ_niv1_code,
        categ_niv1_lib,
        categ_domaine,
        esms,
        esms_capaTot_inst,
        esms_capaInternat_inst,
        esms_esh,
        esms_ash,
        esms_pa,
        san,
        san_med,
        san_chir,
        san_obs,
        san_psy,
        san_sld,
        san_urg,
        san_dialyse,
        san_cancer,
        nb_scanners,
        nb_irm,
        gestion_ars,
        gestion_dreets,
        gestion_drihl,
        version_nomenclature,
        tutelle,
        mft_code,
        mft_lib,
        sph_code,
        sph_lib,
        geoloc_source,
        geoloc_precision,
        geoloc_legal_x,
        geoloc_legal_y,
        geoloc_legal_projection,
        geoloc_3857_x,
        geoloc_3857_y,
        geoloc_4326_long,
        geoloc_4326_lat
    FROM "staging"."public"."staging__sa_t_finess"
)

SELECT 
    finess.*, 
    c.libelle AS commune_libelle,
    d.libelle AS departement_libelle
FROM finess
LEFT JOIN "staging"."public"."staging__v_commune" c ON finess.com_code = c.com
LEFT JOIN "staging"."public"."staging__v_departement" d ON SUBSTRING(finess.com_code, 1, 2) = d.dep
  );