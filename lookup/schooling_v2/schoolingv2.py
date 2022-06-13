import pymysql
from sqlalchemy import create_engine
from decouple import config
from dotenv import load_dotenv
from pandas import read_sql_query, Int32Dtype


load_dotenv()
# get the environment variables needed
USER = config('USRCaris')
PASSWORD = config('PASSCaris')
HOSTNAME = config('HOSTCaris')
DBNAME = config('DBCaris')


# get the engine to connect and fetch
engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}")

query = '''
SELECT 
    dm.id_patient as id_patient,
    d.case_id,
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
'''

sdata = read_sql_query(query, engine, parse_dates=True)
sdata.id_patient = sdata.id_patient.astype(Int32Dtype())


kap_query = f'''
    SELECT 
    (a.score_7 + a.score_8a + score_9a + score_9b + score_17a + score_21 + score_21a + score_22 + score_22a + 1) AS parenting_score_total,
    p.patient_code AS dreams_code,
    dga.nbr_presence,
    a.*
FROM
    (SELECT 
        CASE
                WHEN
                    d.a7_ak_kils_w_ap_viv_10_14 = '0'
                        OR d.a7_Ak_kiles_w_ap_viv_15_19 = '0'
                THEN
                    2
                WHEN
                    d.a7_ak_kils_w_ap_viv_10_14 = '1'
                        OR d.a7_Ak_kiles_w_ap_viv_15_19 = '1'
                THEN
                    4
                WHEN
                    d.a7_ak_kils_w_ap_viv_10_14 = '001'
                        OR d.a7_Ak_kiles_w_ap_viv_15_19 = '01'
                THEN
                    4
                WHEN
                    d.a7_ak_kils_w_ap_viv_10_14 = '03'
                        OR d.a7_Ak_kiles_w_ap_viv_15_19 = '2'
                THEN
                    4
                WHEN
                    d.a7_ak_kils_w_ap_viv_10_14 = 'lt_presize'
                        OR d.a7_Ak_kiles_w_ap_viv_15_19 = 'lt_presize'
                THEN
                    2
            END AS score_7,
            IF(d.a8_eske_gen_yon_moun_ou_santi_ou_ka_al_kote_ = '0', 0, IF(d.a8_eske_gen_yon_moun_ou_santi_ou_ka_al_kote_ = '2', 4, 0)) AS score_8a,
            IF(d.a9_ou_gen_lot_kote_ou_pa_santi_ou_ansekirite = '1', 2, 0) AS score_9a,
            IF(d.b9_list_kote_ou_pa_an_sekirite IN ('1' , '3', '2', '04'), 4, IF(d.b9_list_kote_ou_pa_an_sekirite = '4', 8, IF(d.b9_list_kote_ou_pa_an_sekirite = '0001', 2, 0))) AS score_9b,
            IF(a17 IN ('4' , '04', '004'), 8, 0) AS score_17a,
            IF(d.eske_ou_konn_bwe_alkol = 'oui', 3, 0) AS score_21,
            IF(d.a21_ske_ou_bw_alkl_osnon_itilize_lt_dwg_10_14 = '3'
                OR a21_nan_ki_frenkans_ou_itilize_alkol = '2', 6, IF(d.a21_ske_ou_bw_alkl_osnon_itilize_lt_dwg_10_14 = '5'
                OR a21_nan_ki_frenkans_ou_itilize_alkol = '5', 8, 0)) AS score_21a,
            IF(d.eske_ou_konn_itilize_dwog = 'oui', 3, 0) AS score_22,
            IF(d.ske_ou_konn_itilize_dwg_10_14 = '3'
                OR ske_ou_konn_itilize_dwg_15_19 = '2', 6, IF(d.ske_ou_konn_itilize_dwg_10_14 = '5'
                OR ske_ou_konn_itilize_dwg_15_19 = '5', 8, 0)) AS score_22a,
            d.*
    FROM
        caris_db.dreams_surveys_data d
    WHERE
        (TIMESTAMPDIFF(YEAR, d.nan_ki_dat_ou_fet, NOW()) BETWEEN 10 AND 17)
            AND (d.a7_ak_kils_w_ap_viv_10_14 NOT IN ('8' , '6')
            OR d.a7_ak_kils_w_ap_viv_10_14 IS NULL)
            AND (d.a7_Ak_kiles_w_ap_viv_15_19 NOT IN ('5' , '3')
            OR d.a7_Ak_kiles_w_ap_viv_15_19 IS NULL)) a
        LEFT JOIN
    dream_member dm ON dm.case_id = a.case_id
        LEFT JOIN
    patient p ON p.id = dm.id_patient
        LEFT JOIN
    (SELECT 
        count(distinct dgs.topic) AS nbr_presence, id_patient
    FROM
        dream_group_attendance dga1
        left join dream_group_session dgs on dga1.id_group_session=dgs.id where dga1.value="P"
    GROUP BY dga1.id_patient) dga ON dga.id_patient = dm.id_patient
WHERE
    a.total > 14
ORDER BY (a.score_7 + a.score_8a + score_9a + score_9b + score_17a + score_21 + score_21a + score_22 + score_22a + 1) DESC
'''

parenting = read_sql_query(kap_query, engine, parse_dates=True)
# get the test excel file from Query
parenting.fillna("---", inplace=True)
KAP = parenting[['case_id', 'parenting_score_total', 'dreams_code',
                 'a1_dat_entvyou_a_ft_jjmmaa_egz_010817', 'f_komin']]

# close the pool of connection
engine.dispose()
