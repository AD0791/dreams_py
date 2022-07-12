
def type_test(tt):
    if tt == '0,':
        return "sanguin"
    elif ((tt == '1,') | (tt == '0,,1,')):
        return 'autotest'
    elif(tt == 'no'):
        return 'no_info'
    else:
        return 'verify_me'


def vih_autotest_result(var):
    if var == 'no':
        return 'no_info'
    elif(
        var == '0,'
    ):
        return 'indeterminee'
    elif(
        (var == '0,,1,') |
        (var == '1,')
    ):
        return 'non_reactif'
    elif(
        (var == '2,') |
        (var == '0,,2,')
    ):
        return 'reactif'
    else:
        'verify_me'


def vih_test_result(vtr):
    if vtr == '0,':
        return 'no_info'
    elif(
        (vtr == '0,,3,') |
        (vtr == '0,,2,,3,') |
        (vtr == '2,,3,') |
        (vtr == '3,')
    ):
        return 'indetermine'
    elif(
        (vtr == '0,,2,') |
        (vtr == '2,')
    ):
        return 'negatif'
    elif(
        (vtr == '0,,1,') |
        (vtr == '1,')
    ):
        return 'positif'
    else:
        return 'verify_me'


def curriculum_detailed(pres):
    if pres >= 17:
        return "yes"
    elif 1 <= pres <= 16:
        return "has_started"
    else:
        return "no"


def curriculum(pres):
    return "yes" if pres >= 17 else "no"


def parenting_detailed(pres):
    if pres >= 12:
        return "yes"
    elif 1 <= pres <= 11:
        return "has_started"
    else:
        return "no"


def parenting(pres):
    return "yes" if pres >= 12 else "no"


def condom(df):
    return "yes" if (df.has_comdom_topic == "yes" or df.number_of_condoms_sensibilize > 0 or df.number_condoms_reception_in_the_interval > 0 or df.number_condoms_sensibilization_date_in_the_interval > 0) else "no"


def hts_awareness(ha):
    return 'yes' if ha > 0 else 'no'


def treatment_debut(tdebut):
    return 'yes' if tdebut > 0 else 'no'


def hts(hd):
    return "yes" if hd > 0 else "no"


def vbg(vbg):
    return "yes" if vbg > 0 else "no"


def gyneco(gyneco):
    return "yes" if gyneco > 0 else "no"


def prep_awareness(pa):
    return "yes" if pa > 0 else "no"


def prep_reference(pr):
    return "yes" if pr > 0 else "no"


def prep(pd):
    return "yes" if pd > 0 else "no"


def contraceptive_awareness(caw):
    return 'yes' if caw > 0 else 'no'


def contraceptive(cd):
    return "yes" if cd > 0 else "no"


def postcare(df):
    return "yes" if (df.number_vbg_treatment_date_in_the_interval > 0 or df.number_gynecological_care_date_in_the_interval > 0) else "no"


def socioeco(df):
    return "yes" if ((df.muso == "yes") or (df.gardening == "yes")) else "no"


def prim_1014(df):
    return "primary" if (df.age_range == "10-14" and df.curriculum == "yes") else "no"


def prim_1519(df):
    return "primary" if (df.age_range == "15-19" and df.curriculum == "yes" and df.condom == "yes") else "no"


def prim_2024(df):
    return "primary" if (df.age_range == "20-24" and df.curriculum == "yes" and df.condom == "yes") else "no"


"""
def sec_1014(df):
    return "secondary" if (df.age_range=="10-14" and ((df.condom=="yes")|(df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"

def sec_1519(df):
    return "secondary" if (df.age_range=="15-19" and ((df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"

def sec_2024(df):
    return "secondary" if (df.age_range=="20-24" and ((df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"



def comp_1014(df):
    return "complete" if (df.age_range=="10-14" and  df.curriculum=="no" and ((df.condom=="yes")|(df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"

def comp_1519(df):
    return "complete" if (df.age_range=="15-19" and  (((df.curriculum=="yes")&(df.condom=="no"))|((df.curriculum=="no")&(df.condom=="yes"))) and ((df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"

def comp_2024(df):
    return "complete" if (df.age_range=="20-24" and  (((df.curriculum=="yes")&(df.condom=="no"))|((df.curriculum=="no")&(df.condom=="yes"))) and ((df.hts=="yes")|(df.post_violence_care=="yes")|(df.socioeco_app=="yes")|(df.prep=="yes"))) else "no"


"""
