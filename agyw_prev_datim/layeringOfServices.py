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
dreams_mastersheet = pd.read_sql_query(query,engine,parse_dates=True)
# close the pool of connection
engine.dispose()
# turn to integer
dreams_mastersheet.age = dreams_mastersheet.age.fillna(-1000)
dreams_mastersheet.age = dreams_mastersheet.age.astype(int16)

dreams_mastersheet['age_range'] = dreams_mastersheet.age.map(tranche_age_classique)
dreams_mastersheet['newage_range'] = dreams_mastersheet.age.map(tranche_age_mineur_majeur)


dreams_mastersheet["date_entevyou"] = pd.to_datetime( dreams_mastersheet.a1_dat_entvyou_a_ft_jjmmaa_egz_010817)

dreams_mastersheet["fiscal_year"] = dreams_mastersheet.date_entevyou.map(fiscalYear21)
dreams_mastersheet["timeOn_system"] = dreams_mastersheet.date_entevyou.map(validTimeOnSystem)
dreams_mastersheet["months_now_dateEntevyou"] = dreams_mastersheet.date_entevyou.map(between_now_date_entevyou)
dreams_mastersheet["agyw_period_range"] = dreams_mastersheet.months_now_dateEntevyou.map(agywPeriods)

dreams_mastersheet.number_of_different_topic = dreams_mastersheet.number_of_different_topic.fillna(-1000)
dreams_mastersheet.number_of_different_topic = dreams_mastersheet.number_of_different_topic.astype(int16)


dreams_mastersheet['curriculum_servis_auMoins_1fois'] = dreams_mastersheet.number_of_different_topic.map(curriculum_atLeastOneService)
dreams_mastersheet['curriculum'] = dreams_mastersheet.number_of_different_topic.map(status_curriculum)


dreams_mastersheet.first_session_date = dreams_mastersheet.first_session_date.fillna('0000-00-00')
dreams_mastersheet.last_session_date = dreams_mastersheet.last_session_date.fillna('0000-00-00')

dreams_mastersheet["curriculum_date_debut"] = pd.to_datetime( dreams_mastersheet.first_session_date,errors='coerce')
dreams_mastersheet["curriculum_date_end"] = pd.to_datetime( dreams_mastersheet.last_session_date,errors='coerce')

dreams_mastersheet['curriculum_date_debut_fy'] = dreams_mastersheet.curriculum_date_debut.map(id_quarter_services)
dreams_mastersheet['curriculum_date_end_fy'] = dreams_mastersheet.curriculum_date_end.map(id_quarter_services)




dreams_mastersheet.last_hiv_test_date = dreams_mastersheet.last_hiv_test_date.fillna('0000-00-00')
dreams_mastersheet["hts_date"] = pd.to_datetime( dreams_mastersheet.last_hiv_test_date,errors='coerce')

dreams_mastersheet.last_condoms_reception_date = dreams_mastersheet.last_condoms_reception_date.fillna('0000-00-00')
dreams_mastersheet['condoms_date'] = pd.to_datetime(dreams_mastersheet.last_condoms_reception_date,errors='coerce')

dreams_mastersheet.last_vbg_treatment_date = dreams_mastersheet.last_vbg_treatment_date.fillna('0000-00-00')
dreams_mastersheet['vbg_date'] = pd.to_datetime(dreams_mastersheet.last_vbg_treatment_date,errors='coerce')

dreams_mastersheet.last_gynecological_care_date = dreams_mastersheet.last_gynecological_care_date.fillna('0000-00-00')
dreams_mastersheet['gyneco_date'] = pd.to_datetime(dreams_mastersheet.last_gynecological_care_date,errors='coerce')


dreams_mastersheet['hts'] = dreams_mastersheet.hts_date.map(hcvg_valid_services)
dreams_mastersheet['condoms'] = dreams_mastersheet.condoms_date.map(hcvg_valid_services)
dreams_mastersheet['vbg'] = dreams_mastersheet.vbg_date.map(hcvg_valid_services)
dreams_mastersheet['gyneco'] = dreams_mastersheet.gyneco_date.map(hcvg_valid_services)

dreams_mastersheet['hts_fy'] = dreams_mastersheet.hts_date.map(id_quarter_services)
dreams_mastersheet['condoms_fy'] = dreams_mastersheet.condoms_date.map(id_quarter_services)
dreams_mastersheet['vbg_fy'] = dreams_mastersheet.vbg_date.map(id_quarter_services)
dreams_mastersheet['gyneco_fy'] = dreams_mastersheet.gyneco_date.map(id_quarter_services)

dreams_mastersheet['post_care_treatment'] = dreams_mastersheet.apply(lambda df: post_care_app(df),axis=1)

dreams_mastersheet['socio_eco_app'] = dreams_mastersheet.apply(lambda df: socioEco_app(df),axis=1)


dreams_mastersheet['recevoir_1services'] = dreams_mastersheet.apply(lambda df: unServiceDreams(df),axis=1)


dreams_mastersheet['ps_10_14'] = dreams_mastersheet.apply(lambda df: service_primaire_10_14(df),axis=1)
dreams_mastersheet['ps_15_19'] = dreams_mastersheet.apply(lambda df: service_primaire_15_19(df), axis=1)
dreams_mastersheet['ps_20_24'] = dreams_mastersheet.apply(lambda df: service_primaire_20_24(df), axis=1)

dreams_mastersheet['score_eligible_AGYW'] = dreams_mastersheet.total.map(isAGYW)


























































































