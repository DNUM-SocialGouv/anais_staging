
  
  create view "duckdb_database"."main"."staging__helios_siicea_missions__dbt_tmp" as (
    

WITH missions AS (
    SELECT 
        *,
        CASE 
            WHEN LENGTH("Code FINESS") = 8 THEN '0' || "Code FINESS"
            ELSE "Code FINESS"
        END AS cd_finess
    FROM "duckdb_database"."main"."staging__sa_siicea_missions"
    WHERE "Code thème IGAS" IN (
        'MS634D13', 'MS634N1', 'MS634E1', 'MS634D12', 'MS634R1',
        'MS634D11', 'MS634D15', 'MS634C10'
    )
    AND "Type de mission" NOT IN (
        'Audit', 'Audit franco-wallon', 'Evaluation', 
        'Visites de conformité', 'Enquête administrative'
    )
    AND "Statut de la mission" IN ('Clôturé', 'Maintenu')
    AND (
        CAST(SUBSTRING("Date réelle ""Visite""", 1, 4) || 
             SUBSTRING("Date réelle ""Visite""", 6, 2) || 
             SUBSTRING("Date réelle ""Visite""", 9, 2) AS INTEGER)
        BETWEEN 20220101 AND 20241231
    )
)

SELECT * FROM missions
  );
