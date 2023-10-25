from numpy import nan
import pymysql
from sqlalchemy import create_engine, text
from decouple import config
from dotenv import load_dotenv
from pandas import read_sql_query, Int32Dtype, to_datetime


load_dotenv("../old_caris_host.env")
# get the environment variables needed
USER = config('USRCaris')
PASSWORD = config('PASSCaris')
HOSTNAME = config('HOSTCaris')
DBNAME = config('DBCaris')
# get the engine to connect and fetch
engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}")

_sdata_query = f"""
SELECT
    dh.office,
    d.case_id,
    dm.id_patient AS id_patient,
    p.patient_code AS code,
    d.a_non_patisipan_an AS first_name,
    d.b_siyati AS last_name,
    TIMESTAMPDIFF(YEAR,
        d.nan_ki_dat_ou_fet,
        NOW()) AS age,
    IF(TIMESTAMPDIFF(YEAR,
            d.nan_ki_dat_ou_fet,
            NOW()) >= 10
            AND TIMESTAMPDIFF(YEAR,
            d.nan_ki_dat_ou_fet,
            NOW()) <= 14,
        '10-14',
        IF(TIMESTAMPDIFF(YEAR,
                d.nan_ki_dat_ou_fet,
                NOW()) >= 15
                AND TIMESTAMPDIFF(YEAR,
                d.nan_ki_dat_ou_fet,
                NOW()) <= 19,
            '15-19',
            IF(TIMESTAMPDIFF(YEAR,
                    d.nan_ki_dat_ou_fet,
                    NOW()) >= 20
                    AND TIMESTAMPDIFF(YEAR,
                    d.nan_ki_dat_ou_fet,
                    NOW()) <= 24,
                '20-24',
                IF(TIMESTAMPDIFF(YEAR,
                        d.nan_ki_dat_ou_fet,
                        NOW()) >= 25
                        AND TIMESTAMPDIFF(YEAR,
                        d.nan_ki_dat_ou_fet,
                        NOW()) <= 29,
                    '25-29',
                    'not_valid_age')))) AS age_range,
    IF(TIMESTAMPDIFF(YEAR,
            d.nan_ki_dat_ou_fet,
            NOW()) >= 10
            AND TIMESTAMPDIFF(YEAR,
            d.nan_ki_dat_ou_fet,
            NOW()) <= 14,
        '10-14',
        IF(TIMESTAMPDIFF(YEAR,
                d.nan_ki_dat_ou_fet,
                NOW()) >= 15
                AND TIMESTAMPDIFF(YEAR,
                d.nan_ki_dat_ou_fet,
                NOW()) <= 17,
            '15-17',
            IF(TIMESTAMPDIFF(YEAR,
                    d.nan_ki_dat_ou_fet,
                    NOW()) >= 18
                    AND TIMESTAMPDIFF(YEAR,
                    d.nan_ki_dat_ou_fet,
                    NOW()) <= 24,
                '18-24',
                IF(TIMESTAMPDIFF(YEAR,
                        d.nan_ki_dat_ou_fet,
                        NOW()) >= 25
                        AND TIMESTAMPDIFF(YEAR,
                        d.nan_ki_dat_ou_fet,
                        NOW()) <= 29,
                    '25-29',
                    'not_valid_age')))) AS ovc_age,
    d.nan_ki_dat_ou_fet AS dob,
    IF(TIMESTAMPDIFF(MONTH,
            d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
            NOW()) >= 0
            AND TIMESTAMPDIFF(MONTH,
            d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
            NOW()) <= 6,
        '0-6 months',
        IF(TIMESTAMPDIFF(MONTH,
                d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
                NOW()) >= 7
                AND TIMESTAMPDIFF(MONTH,
                d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
                NOW()) <= 12,
            '07-12 months',
            IF(TIMESTAMPDIFF(MONTH,
                    d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
                    NOW()) >= 13
                    AND TIMESTAMPDIFF(MONTH,
                    d.a1_dat_entvyou_a_ft_jjmmaa_egz_010817,
                    NOW()) <= 24,
                '13-24 months',
                '25+ months'))) AS month_in_program_range,
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
    d.organisation,
    d.form_link
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

sdata = read_sql_query(text(_sdata_query), engine.connect(), parse_dates=True)

engine.dispose()


# EDA
sdata.replace(r'^\s*$', nan, regex=True, inplace=True)

sdata.id_patient = sdata.id_patient.astype(Int32Dtype())
sdata.code = sdata.code.fillna('---')
sdata.interview_date = sdata.loc[:, 'interview_date'].apply(
    to_datetime)

to_be_served = sdata[sdata.code == '---']
to_be_served.interview_date = to_be_served.loc[:, 'interview_date'].apply(
    to_datetime)
to_be_served.total = to_be_served.total.astype(Int32Dtype())
to_be_served = to_be_served[to_be_served.total >= 14]

eligible = sdata[sdata.total >= 14]

served = sdata[
    (sdata.code != '---')&
    (sdata.total>=14)
]
