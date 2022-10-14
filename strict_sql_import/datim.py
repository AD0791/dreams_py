

start_date="2021-10-01"
end_date="2022-09-30"

datim_query = f"""
SELECT 
    a.id_patient,
    h.id_parenting_group,
    g.departement,
    g.commune,
    b.nbre_pres_for_inter,
    h.nbre_parenting_coupe_present,
    b.has_comdom_topic,
    b.has_preventive_vbg,
    d.number_of_condoms_sensibilize,
    d.number_condoms_sensibilization_date_in_the_interval,
    d.number_condoms_reception_in_the_interval,
    d.number_hiv_test_awareness_date_in_the_interval,
    d.type_of_test_vih,
    d.number_autotest_date_in_the_interval,
    d.number_test_date_in_the_interval,
    d.test_results,
    autotest_result,
    d.number_hiv_treatment_start_date_in_the_interval,
    d.number_vbg_treatment_date_in_the_interval,
    d.number_gynecological_care_date_in_the_interval,
    d.number_prep_awareness_date_in_the_interval,
    d.number_prep_reference_date_in_the_interval,
    d.number_prep_initiation_date_in_the_interval,
    d.number_contraceptive_sensibilization_date_in_the_interval,
    d.number_contraceptive_reception_in_the_interval,
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
                    '25-29',
                    'not_valid_age')))) AS age_range,
    IF(c.age_in_year >= 10
            AND c.age_in_year <= 14,
        '10-14',
        IF(c.age_in_year >= 15
                AND c.age_in_year <= 17,
            '15-17',
            IF(c.age_in_year >= 18
                    AND c.age_in_year <= 24,
                '18-24',
                IF(c.age_in_year >= 25
                        AND c.age_in_year <= 29,
                    '25-29',
                    'not_valid_age')))) AS ovc_age,
    c.date_interview,
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
    IF(sc.id_patient IS NOT NULL,
        'yes',
        'no') AS has_schooling_payment_in_the_interval,
    IF(e.id_patient IS NOT NULL,
        'yes',
        'no') AS muso,
    IF(f.id_patient IS NOT NULL,
        'yes',
        'no') AS gardening,
    IF(past.id_patient IS NOT NULL,
        'yes',
        'no') AS has_a_service_with_date_in_the_past
FROM
    ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.condom_sensibilization_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.contraceptive_reception_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.test_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.condoms_reception_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.vbg_treatment_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.gynecological_care_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.prep_initiation_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.has_been_sensibilize_for_condom = 1
            AND ((dhi.condom_sensibilization_date BETWEEN '{start_date}' AND '{end_date}')
            OR (dhi.condoms_reception_date BETWEEN '{start_date}' AND '{end_date}')))) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date BETWEEN '{start_date}' AND '{end_date}') UNION (SELECT 
        dpga.id_patient
    FROM
        dream_parenting_group_attendance dpga
    LEFT JOIN dream_parenting_group_session dpgs ON dpgs.id = dpga.id_parenting_group_session
    WHERE
        (dpga.parent_g = 'P'
            OR dpga.parent_vd = 'P'
            OR dpga.yg_g = 'P'
            OR dpga.yg_vd = 'P')
            AND dpgs.date BETWEEN '{start_date}' AND '{end_date}') UNION (SELECT 
        dm.id_patient
    FROM
        dream_member dm
    INNER JOIN patient p ON p.id = dm.id_patient
    INNER JOIN muso_group_members mgm ON mgm.id_patient = dm.id_patient) UNION (SELECT 
        dmx.id_patient
    FROM
        dream_member dmx
    INNER JOIN patient px ON px.id = dmx.id_patient
    INNER JOIN gardening_beneficiary gbx ON gbx.code_dreams = px.patient_code
    GROUP BY dmx.id_patient) UNION (SELECT 
        ds.id_patient
    FROM
        caris_db.dreams_schooling ds
    WHERE
        ds.closed = FALSE
            AND (ds.dat_peyman_fet BETWEEN '{start_date}' AND '{end_date}'))) a
        LEFT JOIN
    (SELECT 
        xy.id_patient,
            COUNT(*) AS nbre_pres_for_inter,
            IF((SUM(number_of_session_s_08) > 0
                OR SUM(number_of_session_s_10) > 0
                OR SUM(number_of_session_s_11) > 0
                OR SUM(number_of_session_s_18) > 0), 'yes', 'no') AS has_comdom_topic,
            IF((SUM(number_of_session_s_14) > 0
                OR SUM(number_of_session_s_16) > 0), 'yes', 'no') AS has_preventive_vbg
    FROM
        (SELECT 
        id_patient,
            SUM(dgs.topic = 8) AS number_of_session_s_08,
            SUM(dgs.topic = 10) AS number_of_session_s_10,
            SUM(dgs.topic = 11) AS number_of_session_s_11,
            SUM(dgs.topic = 18) AS number_of_session_s_18,
            SUM(dgs.topic = 14) AS number_of_session_s_14,
            SUM(dgs.topic = 16) AS number_of_session_s_16
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY dga.id_patient , dgs.topic) xy
    GROUP BY xy.id_patient) b ON b.id_patient = a.id_patient
        LEFT JOIN
    (SELECT 
        dm2.id_patient,
            TIMESTAMPDIFF(MONTH, dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817, NOW()) AS month_in_program,
            TIMESTAMPDIFF(YEAR, dsd.nan_ki_dat_ou_fet, NOW()) AS age_in_year,
            dsd.a1_dat_entvyou_a_ft_jjmmaa_egz_010817 AS date_interview
    FROM
        dream_member dm2
    LEFT JOIN dreams_surveys_data dsd ON dsd.case_id = dm2.case_id) c ON a.id_patient = c.id_patient
        LEFT JOIN
    (SELECT 
        dhi1.id_patient,
            SUM((dhi1.condom_sensibilization_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.condom_sensibilization_date IS NOT NULL)) AS number_condoms_sensibilization_date_in_the_interval,
            SUM((dhi1.contraceptive_reception_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.contraceptive_reception_date IS NOT NULL)) AS number_contraceptive_reception_in_the_interval,
            SUM(dhi1.has_been_sensibilize_for_condom = 1
                AND (dhi1.has_been_sensibilize_for_condom IS NOT NULL)) AS number_of_condoms_sensibilize,
            SUM((dhi1.condoms_reception_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.condoms_reception_date IS NOT NULL)) AS number_condoms_reception_in_the_interval,
            SUM((dhi1.test_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.test_date IS NOT NULL)) AS number_test_date_in_the_interval,
            SUM((dhi1.vbg_treatment_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.vbg_treatment_date IS NOT NULL)) AS number_vbg_treatment_date_in_the_interval,
            SUM((dhi1.gynecological_care_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.gynecological_care_date IS NOT NULL)) AS number_gynecological_care_date_in_the_interval,
            SUM((dhi1.prep_initiation_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.prep_initiation_date IS NOT NULL)) AS number_prep_initiation_date_in_the_interval,
            GROUP_CONCAT(DISTINCT dhi1.test_result, ',') AS test_results,
            SUM((dhi1.prep_awareness_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.prep_awareness_date IS NOT NULL)) AS number_prep_awareness_date_in_the_interval,
            SUM((dhi1.prep_reference_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.prep_reference_date IS NOT NULL)) AS number_prep_reference_date_in_the_interval,
            SUM((dhi1.contraceptive_sensibilization_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.contraceptive_sensibilization_date IS NOT NULL)) AS number_contraceptive_sensibilization_date_in_the_interval,
            SUM((dhi1.hiv_treatment_start_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.hiv_treatment_start_date IS NOT NULL)) AS number_hiv_treatment_start_date_in_the_interval,
            SUM((dhi1.hiv_test_awareness_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.hiv_test_awareness_date IS NOT NULL)) AS number_hiv_test_awareness_date_in_the_interval,
            GROUP_CONCAT(DISTINCT dhi1.type_of_test, ',') AS type_of_test_vih,
            SUM((dhi1.autotest_date BETWEEN '{start_date}' AND '{end_date}')
                AND (dhi1.autotest_date IS NOT NULL)) AS number_autotest_date_in_the_interval,
            GROUP_CONCAT(DISTINCT dhi1.autotest_result, ',') AS autotest_result
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
        LEFT JOIN
    (SELECT 
        dpga.id_patient,
            COUNT(*) AS nbre_parenting_coupe_present,
            dpgs.id_group AS id_parenting_group
    FROM
        dream_parenting_group_attendance dpga
    LEFT JOIN dream_parenting_group_session dpgs ON dpgs.id = dpga.id_parenting_group_session
    WHERE
        (dpga.parent_g = 'P'
            OR dpga.parent_vd = 'P'
            OR dpga.yg_g = 'P'
            OR dpga.yg_vd = 'P')
            AND dpgs.date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY id_patient) h ON h.id_patient = a.id_patient
        LEFT JOIN
    ((SELECT 
        dhi.id_patient
    FROM
        dream_hivinfos dhi
    WHERE
        (dhi.test_date < '{start_date}')
            OR (dhi.condoms_reception_date < '{start_date}')
            OR (dhi.vbg_treatment_date < '{start_date}')
            OR (dhi.gynecological_care_date < '{start_date}')
            OR (dhi.prep_initiation_date < '{start_date}')
            OR (dhi.condom_sensibilization_date < '{start_date}')
            OR (dhi.contraceptive_reception_date < '{start_date}')) UNION (SELECT 
        dga.id_patient
    FROM
        dream_group_attendance dga
    LEFT JOIN dream_group_session dgs ON dgs.id = dga.id_group_session
    WHERE
        dga.value = 'P'
            AND dgs.date < '{start_date}')) past ON past.id_patient = a.id_patient
        LEFT JOIN
    (SELECT 
        ds.id_patient
    FROM
        caris_db.dreams_schooling ds
    WHERE
        ds.closed = FALSE
            AND (ds.dat_peyman_fet BETWEEN '{start_date}' AND '{end_date}')) sc ON sc.id_patient = a.id_patient
"""

