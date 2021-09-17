#import os
#from datetime import datetime
#from datetime import date
import pymysql
from sqlalchemy import create_engine
from decouple import config 
from dotenv import load_dotenv
import pandas as pd
from numpy import int16
from enum import Enum

load_dotenv()

# get the environment variables needed
USER= config('USRCaris')
PASSWORD= config('PASSCaris')
HOSTNAME= config('HOSTCaris')
DBNAME= config('DBCaris')

class Set_date(Enum):
   master_start = "2017-10-01"
   master_end= "2021-09-30"
   period_start = "2020-10-01"
   period_end = "2021-09-30"
   past_start = "2019-10-01"
   past_end = "2020-09-30"

# get the engine to connect and fetch
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOSTNAME}/{DBNAME}")

query_period = f"""
SELECT 
    a.id_patient,
    g.departement,
    g.commune,
    b.nbre_pres_for_inter,
    b.has_comdom_topic,
    d.number_of_condoms_sensibilize,
    d.number_condoms_reception_in_the_interval,
    d.number_test_date_in_the_interval,
    d.number_vbg_treatment_date_in_the_interval,
    d.number_gynecological_care_date_in_the_interval,
    d.number_prep_initiation_date_in_the_interval,
    c.age_in_year,
    IF(c.age_in_year >= 10
            AND c.age_in_year <= 14,
        '10-14',
        IF(c.age_in_year >= 15
                AND c.age_in_year <= 19,
            '15-19',
            IF(c.age_in_year >= 20
                    AND c.age_in_year <= 24,
                '20-24',
                IF(c.age_in_year >= 25
                    AND c.age_in_year <= 29,
                '25-29','not_valid_age')))) AS age_range,
    IF(c.month_in_program >= 0
            AND c.month_in_program <= 6,
        '0-6 months',
        IF(c.month_in_program >= 7
                AND c.month_in_program <= 12,
            '07-12 months',
            IF(c.month_in_program >= 13
                    AND c.month_in_program <= 24,
                '13-24 months',
                '25+ months'))) AS month_in_program_range,
    IF(e.id_patient IS NOT NULL,
        'yes',
        'no') AS muso,
    IF(f.id_patient IS NOT NULL,
        'yes',
        'no') AS gardening,
        if(past.id_patient is not null, "yes","no") as has_a_service_with_date_in_the_past
FROM
    ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
            OR (dhi.condoms_reception_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
            OR (dhi.vbg_treatment_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
            OR (dhi.gynecological_care_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
            OR (dhi.prep_initiation_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
            OR (dhi.has_been_sensibilize_for_condom = 1)) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value="P" and dgs.date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}') UNION (SELECT 
        dm.id_patient
    FROM
        dream_member dm
    INNER JOIN patient p ON p.id = dm.id_patient
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm.id_patient
    ) UNION (SELECT 
        dmx.id_patient
    FROM
        dream_member dmx
    INNER JOIN patient px ON px.id = dmx.id_patient
    INNER JOIN gardening_beneficiary gbx ON gbx.code_dreams = px.patient_code
    GROUP BY dmx.id_patient)) a
        LEFT JOIN
    (SELECT 
        xy.id_patient,
            COUNT(*) AS nbre_pres_for_inter,
            IF((SUM(number_of_session_s_08) > 0
                OR SUM(number_of_session_s_10) > 0
                OR SUM(number_of_session_s_11) > 0
                OR SUM(number_of_session_s_18) > 0), 'yes', 'no') AS has_comdom_topic
    FROM
        (SELECT 
        id_patient,
            SUM(dgs.topic = 8) AS number_of_session_s_08,
            SUM(dgs.topic = 10) AS number_of_session_s_10,
            SUM(dgs.topic = 11) AS number_of_session_s_11,
            SUM(dgs.topic = 18) AS number_of_session_s_18
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}'
    GROUP BY dga.id_patient , dgs.topic) xy
    GROUP BY xy.id_patient) b ON b.id_patient = a.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient,
            TIMESTAMPDIFF(MONTH, dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817, NOW()) AS month_in_program,
            TIMESTAMPDIFF(YEAR, dsd.nan_ki_dat_ou_fet, NOW()) AS age_in_year
    FROM
        dream_member dm2
    LEFT JOIN dreams_surveys_data dsd ON dsd.case_id = dm2.case_id) c ON a.id_patient = c.id_patient
        LEFT JOIN
    (SELECT 
        dhi1.id_patient,
            SUM(dhi1.has_been_sensibilize_for_condom = 1
                AND (dhi1.has_been_sensibilize_for_condom IS NOT NULL)) AS number_of_condoms_sensibilize,
            SUM((dhi1.condoms_reception_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
                AND (dhi1.condoms_reception_date IS NOT NULL)) AS number_condoms_reception_in_the_interval,
            SUM((dhi1.test_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
                AND (dhi1.test_date IS NOT NULL)) AS number_test_date_in_the_interval,
            SUM((dhi1.vbg_treatment_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
                AND (dhi1.vbg_treatment_date IS NOT NULL)) AS number_vbg_treatment_date_in_the_interval,
            SUM((dhi1.gynecological_care_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
                AND (dhi1.gynecological_care_date IS NOT NULL)) AS number_gynecological_care_date_in_the_interval,
            SUM((dhi1.prep_initiation_date BETWEEN '{Set_date.period_start.value}' AND '{Set_date.period_end.value}')
                AND (dhi1.prep_initiation_date IS NOT NULL)) AS number_prep_initiation_date_in_the_interval
    FROM
        dream_hivinfos dhi1
    GROUP BY dhi1.id_patient) d ON a.id_patient = d.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient
    FROM
        dream_member dm2
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm2.id_patient
    GROUP BY dm2.id_patient) e ON a.id_patient = e.id_patient
        LEFT JOIN
    (SELECT 
        dm3.id_patient
    FROM
        dream_member dm3
    INNER JOIN patient p1 ON p1.id = dm3.id_patient
    INNER JOIN gardening_beneficiary gb ON gb.code_dreams = p1.patient_code
    GROUP BY dm3.id_patient) f ON a.id_patient = f.id_patient
        LEFT JOIN
    (SELECT 
        dmy.id_patient, lc.name AS commune, ld.name AS departement
    FROM
        dream_member dmy
    LEFT JOIN dream_group dg ON dg.id = dmy.id_group
    LEFT JOIN dream_hub dh ON dh.id = dg.id_dream_hub
    LEFT JOIN lookup_commune lc ON lc.id = dh.commune
    LEFT JOIN lookup_departement ld ON ld.id = lc.departement) g ON a.id_patient = g.id_patient
    
    LEFT JOIN ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date <'{Set_date.period_start.value}' )
            OR (dhi.condoms_reception_date < '{Set_date.period_start.value}')
            OR (dhi.vbg_treatment_date < '{Set_date.period_start.value}')
            OR (dhi.gynecological_care_date < '{Set_date.period_start.value}')
            OR (dhi.prep_initiation_date < '{Set_date.period_start.value}')
            ) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value="P" and dgs.date < '{Set_date.period_start.value}') 
    
    ) past on past.id_patient=a.id_patient
"""








query_master = f"""
SELECT 
    a.id_patient,
    g.departement,
    g.commune,
    b.nbre_pres_for_inter,
    b.has_comdom_topic,
    d.number_of_condoms_sensibilize,
    d.number_condoms_reception_in_the_interval,
    d.number_test_date_in_the_interval,
    d.number_vbg_treatment_date_in_the_interval,
    d.number_gynecological_care_date_in_the_interval,
    d.number_prep_initiation_date_in_the_interval,
    c.age_in_year,
    IF(c.age_in_year >= 10
            AND c.age_in_year <= 14,
        '10-14',
        IF(c.age_in_year >= 15
                AND c.age_in_year <= 19,
            '15-19',
            IF(c.age_in_year >= 20
                    AND c.age_in_year <= 24,
                '20-24',
                IF(c.age_in_year >= 25
                    AND c.age_in_year <= 29,
                '25-29','not_valid_age')))) AS age_range,
    IF(c.month_in_program >= 0
            AND c.month_in_program <= 6,
        '0-6 months',
        IF(c.month_in_program >= 7
                AND c.month_in_program <= 12,
            '07-12 months',
            IF(c.month_in_program >= 13
                    AND c.month_in_program <= 24,
                '13-24 months',
                '25+ months'))) AS month_in_program_range,
    IF(e.id_patient IS NOT NULL,
        'yes',
        'no') AS muso,
    IF(f.id_patient IS NOT NULL,
        'yes',
        'no') AS gardening,
        if(past.id_patient is not null, "yes","no") as has_a_service_with_date_in_the_past
FROM
    ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
            OR (dhi.condoms_reception_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
            OR (dhi.vbg_treatment_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
            OR (dhi.gynecological_care_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
            OR (dhi.prep_initiation_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
            OR (dhi.has_been_sensibilize_for_condom = 1)) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value="P" and dgs.date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}') UNION (SELECT 
        dm.id_patient
    FROM
        dream_member dm
    INNER JOIN patient p ON p.id = dm.id_patient
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm.id_patient
    ) UNION (SELECT 
        dmx.id_patient
    FROM
        dream_member dmx
    INNER JOIN patient px ON px.id = dmx.id_patient
    INNER JOIN gardening_beneficiary gbx ON gbx.code_dreams = px.patient_code
    GROUP BY dmx.id_patient)) a
        LEFT JOIN
    (SELECT 
        xy.id_patient,
            COUNT(*) AS nbre_pres_for_inter,
            IF((SUM(number_of_session_s_08) > 0
                OR SUM(number_of_session_s_10) > 0
                OR SUM(number_of_session_s_11) > 0
                OR SUM(number_of_session_s_18) > 0), 'yes', 'no') AS has_comdom_topic
    FROM
        (SELECT 
        id_patient,
            SUM(dgs.topic = 8) AS number_of_session_s_08,
            SUM(dgs.topic = 10) AS number_of_session_s_10,
            SUM(dgs.topic = 11) AS number_of_session_s_11,
            SUM(dgs.topic = 18) AS number_of_session_s_18
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}'
    GROUP BY dga.id_patient , dgs.topic) xy
    GROUP BY xy.id_patient) b ON b.id_patient = a.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient,
            TIMESTAMPDIFF(MONTH, dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817, NOW()) AS month_in_program,
            TIMESTAMPDIFF(YEAR, dsd.nan_ki_dat_ou_fet, NOW()) AS age_in_year
    FROM
        dream_member dm2
    LEFT JOIN dreams_surveys_data dsd ON dsd.case_id = dm2.case_id) c ON a.id_patient = c.id_patient
        LEFT JOIN
    (SELECT 
        dhi1.id_patient,
            SUM(dhi1.has_been_sensibilize_for_condom = 1
                AND (dhi1.has_been_sensibilize_for_condom IS NOT NULL)) AS number_of_condoms_sensibilize,
            SUM((dhi1.condoms_reception_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
                AND (dhi1.condoms_reception_date IS NOT NULL)) AS number_condoms_reception_in_the_interval,
            SUM((dhi1.test_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
                AND (dhi1.test_date IS NOT NULL)) AS number_test_date_in_the_interval,
            SUM((dhi1.vbg_treatment_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
                AND (dhi1.vbg_treatment_date IS NOT NULL)) AS number_vbg_treatment_date_in_the_interval,
            SUM((dhi1.gynecological_care_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
                AND (dhi1.gynecological_care_date IS NOT NULL)) AS number_gynecological_care_date_in_the_interval,
            SUM((dhi1.prep_initiation_date BETWEEN '{Set_date.master_start.value}' AND '{Set_date.master_end.value}')
                AND (dhi1.prep_initiation_date IS NOT NULL)) AS number_prep_initiation_date_in_the_interval
    FROM
        dream_hivinfos dhi1
    GROUP BY dhi1.id_patient) d ON a.id_patient = d.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient
    FROM
        dream_member dm2
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm2.id_patient
    GROUP BY dm2.id_patient) e ON a.id_patient = e.id_patient
        LEFT JOIN
    (SELECT 
        dm3.id_patient
    FROM
        dream_member dm3
    INNER JOIN patient p1 ON p1.id = dm3.id_patient
    INNER JOIN gardening_beneficiary gb ON gb.code_dreams = p1.patient_code
    GROUP BY dm3.id_patient) f ON a.id_patient = f.id_patient
        LEFT JOIN
    (SELECT 
        dmy.id_patient, lc.name AS commune, ld.name AS departement
    FROM
        dream_member dmy
    LEFT JOIN dream_group dg ON dg.id = dmy.id_group
    LEFT JOIN dream_hub dh ON dh.id = dg.id_dream_hub
    LEFT JOIN lookup_commune lc ON lc.id = dh.commune
    LEFT JOIN lookup_departement ld ON ld.id = lc.departement) g ON a.id_patient = g.id_patient
    
    LEFT JOIN ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date <'{Set_date.master_start.value}' )
            OR (dhi.condoms_reception_date < '{Set_date.master_start.value}')
            OR (dhi.vbg_treatment_date < '{Set_date.master_start.value}')
            OR (dhi.gynecological_care_date < '{Set_date.master_start.value}')
            OR (dhi.prep_initiation_date < '{Set_date.master_start.value}')
            ) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value="P" and dgs.date < '{Set_date.master_start.value}') 
    
    ) past on past.id_patient=a.id_patient
"""


query_mastersheet= """
SELECT 
    dm.id_patient as id_patient,
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
    MAX(dhi.hiv_test_awareness_date) AS last_sensibilisation_hiv_test_date,
    GROUP_CONCAT(distinct dhi.hiv_test_acceptation, ',') acceptation_hiv_test,
    GROUP_CONCAT(DISTINCT dhi.test_result, ',') AS test_results,
    GROUP_CONCAT(DISTINCT ltlr.name, ',') AS test_results_with_label,
    MAX(dhi.condoms_reception_date) AS last_condoms_reception_date,
    group_concat(distinct dhi.has_been_sensibilize_for_condom, ',') as sensibilisation_condom,
    group_concat(distinct dhi.accept_condom, ',') as acceptation_condom,
    MAX(dhi.vbg_treatment_date) AS last_vbg_treatment_date,
    MAX(dhi.gynecological_care_date) AS last_gynecological_care_date,
    MAX(dhi.prep_awareness_date) AS last_sensibilisation_prep_date,
    MAX(dhi.prep_reference_date) AS last_reference_prep_date,
    MAX(dhi.prep_initiation_date) AS last_initiation_prep_date,
    GROUP_CONCAT(distinct dhi.prep_acceptation, ',') AS acceptation_prep,
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
"""



agyw_served_period = pd.read_sql_query(query_period,engine,parse_dates=True)
agyw_served = pd.read_sql_query(query_master,engine,parse_dates=True)
mastersheet_served = pd.read_sql_query(query_mastersheet,engine,parse_dates=True)

# close the pool of connection
engine.dispose()


agyw_served_period.to_excel('agyw_served_period.xlsx',index=False,na_rep="")
agyw_served.to_excel('agyw_served.xlsx',index=False,na_rep="")
mastersheet_served.to_excel('mastersheet_served.xlsx',index=False,na_rep="")







#### option route


"""

#agyw_served_past.to_excel('agyw_served_past.xlsx',index=False,na_rep="")
#agyw_served_past = pd.read_sql_query(query_past,engine,parse_dates=True)



query_agyw_service_session = 
SELECT p.patient_code as Code,
	b.last_name,
    b.first_name,
    b.dob,
    TIMESTAMPDIFF(YEAR, b.dob, NOW()) AS age,
    count(*) as nbre_session,
    sum(a.session_for_inter>0) as number_of_different_topic_for_inter,
    a.id_patient,
    sum(a.topic=08) as number_of_session_sensiblisation_08_for_all_time,
    sum(a.topic=10) as number_of_session_sensiblisation_10_for_all_time,
    sum(a.topic=11) as number_of_session_sensiblisation_11_for_all_time,
    sum(a.topic=18) as number_of_session_sensibilisation_18_for_all_time,
    sum(a.number_of_session_s_08>0) as number_of_session_sensibilisation_08_for_inter,
    sum(a.number_of_session_s_10>0) as number_of_session_sensibilisation_10_for_inter,
    sum(a.number_of_session_s_11>0) as number_of_session_sensibilisation_11_for_inter,
    sum(a.number_of_session_s_18>0) as number_of_session_sensibilisation_18_for_inter,
    dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817 as interview_date
FROM
(select *,sum(dgs.date between '{Set_date.start_date.value}' and '{Set_date.end_date.value}') as session_for_inter,
    sum( dgs.topic=08  and (dgs.date between '{Set_date.start_date.value}' and '{Set_date.end_date.value}')) as number_of_session_s_08,
    sum(dgs.topic=10 and (dgs.date between '{Set_date.start_date.value}' and '{Set_date.end_date.value}')) as number_of_session_s_10,
    sum(dgs.topic=11 and (dgs.date between '{Set_date.start_date.value}' and '{Set_date.end_date.value}')) as number_of_session_s_11,
    sum(dgs.topic=18 and (dgs.date between '{Set_date.start_date.value}' and '{Set_date.end_date.value}')) as number_of_session_s_18
 from 
    dream_group_attendance dga
        LEFT JOIN
    dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
    dga.value = 'P'  
    group by dga.id_patient,dgs.topic
    ) a
        LEFT JOIN
    patient p ON p.id = a.id_patient
        LEFT JOIN
    beneficiary b ON a.id_patient = b.id_patient
    left join dream_member dm on dm.id_patient=a.id_patient
    left join dreams_surveys_data dsd  on dsd.case_id=dm.case_id
group by a.id_patient
having number_of_different_topic_for_inter>=1
# -- having number_of_different_topic_for_inter>=1




query_agyw_service_hivinfo =
SELECT 
    p.patient_code AS Code,
    b.last_name,
    b.first_name,
    b.dob,
    TIMESTAMPDIFF(YEAR, b.dob, NOW()) AS age,
    dhi.test_date,
    dhi.condoms_reception_date,
    dhi.vbg_treatment_date,
    dhi.gynecological_care_date,
    dhi.prep_initiation_date,
    dhi.has_been_sensibilize_for_condom,
    dsd.total AS score,
    dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817 AS enrollement_date,
    IF(mgm.id_patient IS NOT NULL,
        'yes',
        'no') AS is_muso,
    IF(gb.case_id IS NOT NULL, 'yes', 'no') AS is_gardening
FROM
    dream_hivinfos dhi
        LEFT JOIN
    patient p ON p.id = dhi.id_patient
        LEFT JOIN
    beneficiary b ON dhi.id_patient = b.id_patient
        LEFT JOIN
    dream_member dm ON dm.id_patient = dhi.id_patient
        LEFT JOIN
    dreams_surveys_data dsd ON dsd.case_id = dm.case_id
        LEFT JOIN
    muso_group_members mgm ON mgm.id_patient = dm.id_patient
        LEFT JOIN
    gardening_beneficiary gb ON gb.code_dreams = p.patient_code
WHERE ((dhi.test_date BETWEEN '{Set_date.start_date.value}' AND '{Set_date.end_date.value}') OR (dhi.condoms_reception_date BETWEEN '{Set_date.start_date.value}' AND '{Set_date.end_date.value}') OR (dhi.vbg_treatment_date BETWEEN '{Set_date.start_date.value}' AND '{Set_date.end_date.value}') OR (dhi.gynecological_care_date BETWEEN '{Set_date.start_date.value}' AND '{Set_date.end_date.value}') OR (dhi.prep_initiation_date BETWEEN '{Set_date.start_date.value}' AND '{Set_date.end_date.value}') OR (dhi.has_been_sensibilize_for_condom = 1) OR (gb.case_id IS NOT NULL) OR (mgm.id_patient IS NOT NULL))
#mastersheet = pd.read_sql_query(query_mastersheet,engine,parse_dates=True)


query_past = f
SELECT 
    a.id_patient,
    g.departement,
    g.commune,
    b.nbre_pres_for_inter,
    b.has_comdom_topic,
    d.number_of_condoms_sensibilize,
    d.number_condoms_reception_in_the_interval,
    d.number_test_date_in_the_interval,
    d.number_vbg_treatment_date_in_the_interval,
    d.number_gynecological_care_date_in_the_interval,
    d.number_prep_initiation_date_in_the_interval,
    c.age_in_year,
    IF(c.age_in_year >= 10
            AND c.age_in_year <= 14,
        '10-14',
        IF(c.age_in_year >= 15
                AND c.age_in_year <= 19,
            '15-19',
            IF(c.age_in_year >= 20
                    AND c.age_in_year <= 24,
                '20-24',
                IF(c.age_in_year >= 25
                    AND c.age_in_year <= 29,
                '25-29','not_valid_age')))) AS age_range,
    IF(c.month_in_program >= 0
            AND c.month_in_program <= 6,
        '0-6 months',
        IF(c.month_in_program >= 7
                AND c.month_in_program <= 12,
            '07-12 months',
            IF(c.month_in_program >= 13
                    AND c.month_in_program <= 24,
                '13-24 months',
                '25+ months'))) AS month_in_program_range,
    IF(e.id_patient IS NOT NULL,
        'yes',
        'no') AS muso,
    IF(f.id_patient IS NOT NULL,
        'yes',
        'no') AS gardening
FROM
    ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
            OR (dhi.condoms_reception_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
            OR (dhi.vbg_treatment_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
            OR (dhi.gynecological_care_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
            OR (dhi.prep_initiation_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
            OR (dhi.has_been_sensibilize_for_condom = 1)) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dgs.date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}') UNION (SELECT 
        dm.id_patient
    FROM
        dream_member dm
    LEFT JOIN patient p ON p.id = dm.id_patient
    LEFT JOIN muso_group_members mgm ON mgm.id_patient = dm.id_patient
    WHERE
        (mgm.id_patient IS NOT NULL)) UNION (SELECT 
        dmx.id_patient
    FROM
        dream_member dmx
    INNER JOIN patient px ON px.id = dmx.id_patient
    INNER JOIN gardening_beneficiary gbx ON gbx.code_dreams = px.patient_code
    GROUP BY dmx.id_patient)) a
        LEFT JOIN
    (SELECT 
        xy.id_patient,
            COUNT(*) AS nbre_pres_for_inter,
            IF((SUM(number_of_session_s_08) > 0
                OR SUM(number_of_session_s_10) > 0
                OR SUM(number_of_session_s_11) > 0
                OR SUM(number_of_session_s_18) > 0), 'yes', 'no') AS has_comdom_topic
    FROM
        (SELECT 
        id_patient,
            SUM(dgs.topic = 8) AS number_of_session_s_08,
            SUM(dgs.topic = 10) AS number_of_session_s_10,
            SUM(dgs.topic = 11) AS number_of_session_s_11,
            SUM(dgs.topic = 18) AS number_of_session_s_18
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}'
    GROUP BY dga.id_patient , dgs.topic) xy
    GROUP BY xy.id_patient) b ON b.id_patient = a.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient,
            TIMESTAMPDIFF(MONTH, dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817, NOW()) AS month_in_program,
            TIMESTAMPDIFF(YEAR, dsd.nan_ki_dat_ou_fet, NOW()) AS age_in_year
    FROM
        dream_member dm2
    LEFT JOIN dreams_surveys_data dsd ON dsd.case_id = dm2.case_id) c ON a.id_patient = c.id_patient
        LEFT JOIN
    (SELECT 
        dhi1.id_patient,
            SUM(dhi1.has_been_sensibilize_for_condom = 1
                AND (dhi1.has_been_sensibilize_for_condom IS NOT NULL)) AS number_of_condoms_sensibilize,
            SUM((dhi1.condoms_reception_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
                AND (dhi1.condoms_reception_date IS NOT NULL)) AS number_condoms_reception_in_the_interval,
            SUM((dhi1.test_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
                AND (dhi1.test_date IS NOT NULL)) AS number_test_date_in_the_interval,
            SUM((dhi1.vbg_treatment_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
                AND (dhi1.vbg_treatment_date IS NOT NULL)) AS number_vbg_treatment_date_in_the_interval,
            SUM((dhi1.gynecological_care_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
                AND (dhi1.gynecological_care_date IS NOT NULL)) AS number_gynecological_care_date_in_the_interval,
            SUM((dhi1.prep_initiation_date BETWEEN '{Set_date.past_start_date.value}' AND '{Set_date.past_end_date.value}')
                AND (dhi1.prep_initiation_date IS NOT NULL)) AS number_prep_initiation_date_in_the_interval
    FROM
        dream_hivinfos dhi1
    GROUP BY dhi1.id_patient) d ON a.id_patient = d.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient
    FROM
        dream_member dm2
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm2.id_patient
    GROUP BY dm2.id_patient) e ON a.id_patient = e.id_patient
        LEFT JOIN
    (SELECT 
        dm3.id_patient
    FROM
        dream_member dm3
    INNER JOIN patient p1 ON p1.id = dm3.id_patient
    INNER JOIN gardening_beneficiary gb ON gb.code_dreams = p1.patient_code
    GROUP BY dm3.id_patient) f ON a.id_patient = f.id_patient
        LEFT JOIN
    (SELECT 
        dmy.id_patient, lc.name AS commune, ld.name AS departement
    FROM
        dream_member dmy
    LEFT JOIN dream_group dg ON dg.id = dmy.id_group
    LEFT JOIN dream_hub dh ON dh.id = dg.id_dream_hub
    LEFT JOIN lookup_commune lc ON lc.id = dh.commune
    LEFT JOIN lookup_departement ld ON ld.id = lc.departement) g ON a.id_patient = g.id_patient



"""