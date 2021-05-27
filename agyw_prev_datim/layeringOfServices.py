#import os
#from datetime import datetime
#from datetime import date
import pymysql
from function import * 
from sqlalchemy import create_engine
from decouple import config 
from dotenv import load_dotenv
import pandas as pd
from numpy import int16
load_dotenv()
# get the environment variables needed
USER= config('USRCaris')
PASSWORD= config('PASSCaris')
HOSTNAME= config('HOSTCaris')
DBNAME= config('DBCaris')
# get the engine to connect and fetch
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}")
query = '''
SELECT 
    dm.id_patient AS main_id,
    IF(TIMESTAMPDIFF(YEAR,
            dsd.nan_ki_dat_ou_fet,
            NOW()) >= 18,
        IF(dsd.a7_Ak_kiles_w_ap_viv_15_19 = '3'
                OR dsd.a1121_aktivite_pouw_rantre_kob_ou_vle_fe = '0'
                or dsd.c6b_Kiles_ki_peye_lekol_ou_Tranche_15_19 = '3'
                or dsd.eske_ou_bay_kob_pou_pran_swen_piti_ou_ayo = 'oui',
            'yes_sup18',
            'no18'),
        if(
			dsd.a7_ak_kils_w_ap_viv_10_14 = '6'
            OR dsd.a1121_aktivite_pouw_rantre_kob_ou_vle_fe = '0'
            OR dsd.c6b_kils_ki_peye_lekl_ou_10_14 = '5'
            or dsd.eske_ou_bay_kob_pou_pran_swen_piti_ou_ayo = 'oui',
            'yes_inf17'
            ,'no17'	)
		) AS muso_eligibility,
    dm.id_group AS actual_id_group,
    b.groups AS group_she_take_sessions,
    p.patient_code AS code,
    ben.last_name,
    ben.first_name,
    ben.dob,
    TIMESTAMPDIFF(YEAR, ben.dob, NOW()) AS age,
    b.pres AS number_of_different_topic,
    b.first_session_date,
    b.last_session_date,
    MAX(dhi.test_date) AS last_hiv_test_date,
    GROUP_CONCAT(DISTINCT dhi.test_result, ',') AS test_results,
    GROUP_CONCAT(DISTINCT ltlr.name, ',') AS test_results_with_label,
    MAX(dhi.condoms_reception_date) AS last_condoms_reception_date,
    MAX(dhi.vbg_treatment_date) AS last_vbg_treatment_date,
    MAX(dhi.gynecological_care_date) AS last_gynecological_care_date,
    dg.name AS actual_group_name,
    dh.name AS actual_hub,
    lc.name AS actual_commune,
    dh.commune AS actual_commune_id,
    ld.name AS actual_departement,
    lc.departement AS actual_departement_id,
    IF(mgm.id_patient IS NOT NULL,
        'yes',
        'no') AS is_muso,
    IF(gb.case_id IS NOT NULL, 'yes', 'no') AS is_gardening,
    tf.*,
    dsd.*
FROM
    dream_member dm
        LEFT JOIN
    (SELECT 
        SUM(a.value = 'P') AS pres,
            a.id_patient,
            GROUP_CONCAT(DISTINCT a.id_group, ',') AS groups,
            MIN(a.date) AS first_session_date,
            MAX(a.date) AS last_session_date
    FROM
        (SELECT 
        dga.*, dgs.topic, dgs.date, dgs.id_group
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
    GROUP BY dga.id_patient , dgs.topic) a
    GROUP BY a.id_patient) b ON b.id_patient = dm.id_patient
        LEFT JOIN
    beneficiary ben ON ben.id_patient = dm.id_patient
        LEFT JOIN
    patient p ON p.id = dm.id_patient
        LEFT JOIN
    caris_db.dream_hivinfos dhi ON dhi.id_patient = dm.id_patient
        LEFT JOIN
    lookup_testing_lab_result ltlr ON ltlr.id = dhi.test_result
        LEFT JOIN
    caris_db.dream_group dg ON dg.id = dm.id_group
        LEFT JOIN
    dream_hub dh ON dh.id = dg.id_dream_hub
        LEFT JOIN
    lookup_commune lc ON lc.id = dh.commune
        LEFT JOIN
    lookup_departement ld ON ld.id = lc.departement
        LEFT JOIN
    dreams_surveys_data dsd ON dsd.case_id = dm.case_id
        LEFT JOIN
    tracking_familymember tf ON tf.id_patient = dm.id_patient
        LEFT JOIN
    muso_group_members mgm ON mgm.id_patient = dm.id_patient
        LEFT JOIN
    gardening_beneficiary gb ON gb.code_dreams = p.patient_code
GROUP BY dm.id_patient
'''
DREAMS_MASTERSHEET = pd.read_sql_query(query,engine,parse_dates=True)
# close the pool of connection
engine.dispose()
# turn to integer
DREAMS_MASTERSHEET.age = DREAMS_MASTERSHEET.age.fillna(-1000)
DREAMS_MASTERSHEET.age = DREAMS_MASTERSHEET.age.astype(int16)

DREAMS_MASTERSHEET['age_range'] = DREAMS_MASTERSHEET.age.map(tranche_age_classique)
DREAMS_MASTERSHEET['newage_range'] = DREAMS_MASTERSHEET.age.map(tranche_age_mineur_majeur)


DREAMS_MASTERSHEET["date_entevyou"] = pd.to_datetime( DREAMS_MASTERSHEET.a1_dat_entvyou_a_ft_jjmmaa_egz_010817)

DREAMS_MASTERSHEET["fiscal_year"] = DREAMS_MASTERSHEET.date_entevyou.map(fiscalYear21)
DREAMS_MASTERSHEET["timeOn_system"] = DREAMS_MASTERSHEET.date_entevyou.map(validTimeOnSystem)
DREAMS_MASTERSHEET["months_now_dateEntevyou"] = DREAMS_MASTERSHEET.date_entevyou.map(between_now_date_entevyou)
DREAMS_MASTERSHEET["agyw_period_range"] = DREAMS_MASTERSHEET.months_now_dateEntevyou.map(agywPeriods)

DREAMS_MASTERSHEET.number_of_different_topic = DREAMS_MASTERSHEET.number_of_different_topic.fillna(-1000)
DREAMS_MASTERSHEET.number_of_different_topic = DREAMS_MASTERSHEET.number_of_different_topic.astype(int16)


DREAMS_MASTERSHEET['curriculum_servis_auMoins_1fois'] = DREAMS_MASTERSHEET.number_of_different_topic.map(curriculum_atLeastOneService)
DREAMS_MASTERSHEET['curriculum'] = DREAMS_MASTERSHEET.number_of_different_topic.map(status_curriculum)

DREAMS_MASTERSHEET["dreams_curriculum"] = DREAMS_MASTERSHEET.curriculum.map(curriculum_condense)

DREAMS_MASTERSHEET.first_session_date = DREAMS_MASTERSHEET.first_session_date.fillna('0000-00-00')
DREAMS_MASTERSHEET.last_session_date = DREAMS_MASTERSHEET.last_session_date.fillna('0000-00-00')

DREAMS_MASTERSHEET["curriculum_date_debut"] = pd.to_datetime( DREAMS_MASTERSHEET.first_session_date,errors='coerce')
DREAMS_MASTERSHEET["curriculum_date_end"] = pd.to_datetime( DREAMS_MASTERSHEET.last_session_date,errors='coerce')

DREAMS_MASTERSHEET['curriculum_date_debut_fy'] = DREAMS_MASTERSHEET.curriculum_date_debut.map(id_quarter_services)
DREAMS_MASTERSHEET['curriculum_date_end_fy'] = DREAMS_MASTERSHEET.curriculum_date_end.map(id_quarter_services)




DREAMS_MASTERSHEET.last_hiv_test_date = DREAMS_MASTERSHEET.last_hiv_test_date.fillna('0000-00-00')
DREAMS_MASTERSHEET["hts_date"] = pd.to_datetime( DREAMS_MASTERSHEET.last_hiv_test_date,errors='coerce')

DREAMS_MASTERSHEET.last_condoms_reception_date = DREAMS_MASTERSHEET.last_condoms_reception_date.fillna('0000-00-00')
DREAMS_MASTERSHEET['condoms_date'] = pd.to_datetime(DREAMS_MASTERSHEET.last_condoms_reception_date,errors='coerce')

DREAMS_MASTERSHEET.last_vbg_treatment_date = DREAMS_MASTERSHEET.last_vbg_treatment_date.fillna('0000-00-00')
DREAMS_MASTERSHEET['vbg_date'] = pd.to_datetime(DREAMS_MASTERSHEET.last_vbg_treatment_date,errors='coerce')

DREAMS_MASTERSHEET.last_gynecological_care_date = DREAMS_MASTERSHEET.last_gynecological_care_date.fillna('0000-00-00')
DREAMS_MASTERSHEET['gyneco_date'] = pd.to_datetime(DREAMS_MASTERSHEET.last_gynecological_care_date,errors='coerce')


DREAMS_MASTERSHEET['hts'] = DREAMS_MASTERSHEET.hts_date.map(hcvg_valid_services)
DREAMS_MASTERSHEET['condoms'] = DREAMS_MASTERSHEET.condoms_date.map(hcvg_valid_services)
DREAMS_MASTERSHEET['vbg'] = DREAMS_MASTERSHEET.vbg_date.map(hcvg_valid_services)
DREAMS_MASTERSHEET['gyneco'] = DREAMS_MASTERSHEET.gyneco_date.map(hcvg_valid_services)

DREAMS_MASTERSHEET['hts_fy'] = DREAMS_MASTERSHEET.hts_date.map(id_quarter_services)
DREAMS_MASTERSHEET['condoms_fy'] = DREAMS_MASTERSHEET.condoms_date.map(id_quarter_services)
DREAMS_MASTERSHEET['vbg_fy'] = DREAMS_MASTERSHEET.vbg_date.map(id_quarter_services)
DREAMS_MASTERSHEET['gyneco_fy'] = DREAMS_MASTERSHEET.gyneco_date.map(id_quarter_services)

DREAMS_MASTERSHEET['post_care_treatment'] = DREAMS_MASTERSHEET.apply(lambda df: post_care_app(df),axis=1)

DREAMS_MASTERSHEET['socio_eco_app'] = DREAMS_MASTERSHEET.apply(lambda df: socioEco_app(df),axis=1)


DREAMS_MASTERSHEET['recevoir_1services'] = DREAMS_MASTERSHEET.apply(lambda df: unServiceDreams(df),axis=1)


DREAMS_MASTERSHEET['ps_10_14'] = DREAMS_MASTERSHEET.apply(lambda df: service_primaire_10_14(df),axis=1)
DREAMS_MASTERSHEET['ps_15_19'] = DREAMS_MASTERSHEET.apply(lambda df: service_primaire_15_19(df), axis=1)
DREAMS_MASTERSHEET['ps_20_24'] = DREAMS_MASTERSHEET.apply(lambda df: service_primaire_20_24(df), axis=1)

DREAMS_MASTERSHEET['score_eligible_AGYW'] = DREAMS_MASTERSHEET.total.map(isAGYW)


























































































