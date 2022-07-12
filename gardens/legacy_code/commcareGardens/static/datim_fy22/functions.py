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


def hts(hd):
    return "yes" if hd > 0 else "no"


def vbg(vbg):
    return "yes" if vbg > 0 else "no"


def gyneco(gyneco):
    return "yes" if gyneco > 0 else "no"


def prep(pd):
    return "yes" if pd > 0 else "no"


def contraceptive(cd):
    return "yes" if cd > 0 else "no"


def postcare(df):
    return "yes" if (df.number_vbg_treatment_date_in_the_interval > 0 or df.number_gynecological_care_date_in_the_interval > 0) else "no"


def socioeco(df):
    return "yes" if (df.muso == "yes" or df.gardening == "yes") else "no"


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
