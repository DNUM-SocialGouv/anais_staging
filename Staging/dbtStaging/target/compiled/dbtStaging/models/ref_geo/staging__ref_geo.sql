

WITH geo AS (
    SELECT
        rc.com AS com_cd,
        rc.libelle AS com_lb,
        rd.dep AS dep_cd,
        rd.libelle AS dep_lb,
        rr.reg AS reg_cd,
        rr.libelle AS reg_lb
    FROM "duckdb_database"."main"."staging__ref_communes" rc
    LEFT JOIN "duckdb_database"."main"."staging__ref_departements" rd
        ON rc.dep = rd.dep
    LEFT JOIN "duckdb_database"."main"."staging__ref_regions" rr
        ON rd.reg = rr.reg
)

SELECT * FROM geo