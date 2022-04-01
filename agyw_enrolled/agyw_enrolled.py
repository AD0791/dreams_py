import pymysql
from sqlalchemy import create_engine
from decouple import config 
from dotenv import load_dotenv
from pandas import read_sql_query

from period import Constante



load_dotenv()
USER= config('USRCaris')
PASSWORD= config('PASSCaris')
HOSTNAME= config('HOSTCaris')
DBNAME= config('DBCaris')

engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}")

total_en_query = f"""
SELECT 
    a.organisation, SUM(a.nbre) AS total
FROM
    (SELECT 
        agent_username,
            COUNT(*) AS nbre,
            dsd.organisation,
            dsd.timeEnd AS entry_date
    FROM
        caris_db.dreams_surveys_data dsd
    WHERE
        dsd.timeEnd >= '{Constante.specific.value}'
            AND dsd.timeEnd <= '{Constante.end.value}'
    GROUP BY agent_username) a
"""

agyw_en_query = f"""
SELECT 
    case_id, agent_username, organisation, timeEnd AS entry_date
FROM
    caris_db.dreams_surveys_data dsd
WHERE
    dsd.timeEnd >= '{Constante.specific.value}'
            AND dsd.timeEnd <= '{Constante.end.value}'
"""

sdata_query = f"""

SELECT 
    d.case_id,
    dm.id_patient as id_patient,
    p.patient_code AS code,
    d.a_non_patisipan_an AS first_name,
    d.b_siyati AS last_name,
    TIMESTAMPDIFF(YEAR,
        d.nan_ki_dat_ou_fet,
        now()) AS age,
    d.nan_ki_dat_ou_fet AS dob,
    d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817 AS interview_date,
    d.e__telefn,
    d.d_adrs AS adress,
    IF(dm.id IS NOT NULL, 'yes', 'no') AS already_in_a_group,
    dm.id_group AS actual_id_group,
    dg.name AS actual_group_name,
    dm.id_parenting_group AS actual_id_parenting_group,
    dpg.name AS actual_parenting_group_name,
    dh.name AS actual_hub,
    ld.name AS actual_departement,
    d.f_komin AS commune,
    d.g_seksyon_kominal AS commune_section,
    d.b1_non_moun_mennen_entvyou_a AS interviewer_firstname,
    d.c1_siyati_moun_ki_f_entvyou_a AS interviewer_lastname,
    d.d1_kad AS interviewer_role,
    d.lot_kad AS interviewer_other_info,
    d.h_kote_entvyou_a_ft AS interview_location,
    d.paran_ou_vivan AS is_your_parent_alive,
    d.i_non_manman AS mothers_name,
    d.j_non_papa AS fathers_name,
    d.k_reskonsab_devan_lalwa AS who_is_your_law_parent,
    d.total,
    d.organisation
FROM
    caris_db.dreams_surveys_data d
        LEFT JOIN
    dream_member dm ON dm.case_id = d.case_id
        LEFT JOIN
    patient p ON p.id = dm.id_patient
        LEFT JOIN
    dream_group dg ON dg.id = dm.id_group
        LEFT JOIN
    dream_group dpg ON dpg.id = dm.id_parenting_group
        LEFT JOIN
    dream_hub dh ON dh.id = dg.id_dream_hub
        LEFT JOIN
    lookup_commune lc ON lc.id = dh.commune
        LEFT JOIN
    lookup_departement ld ON ld.id = lc.departement
"""

total_en = read_sql_query(total_en_query, engine,parse_dates=True)
agyw_en = read_sql_query(agyw_en_query, engine,parse_dates=True)
sdata = read_sql_query(sdata_query, engine,parse_dates=True)

engine.dispose()