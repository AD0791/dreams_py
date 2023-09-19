use caris_db;
SELECT
    p.patient_code,
    dhi.vbg_treatment_date as date_vbg
FROM
    dream_hivinfos dhi
        LEFT JOIN
    patient p ON p.id = dhi.id_patient
WHERE
    vbg_treatment_date >= '2021-10-01'
        AND vbg_treatment_date <= '2022-03-31';