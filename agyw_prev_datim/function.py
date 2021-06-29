from datetime import datetime
from pandas import NaT

# tranche d'age classique pr les services agyw
# tranche age mineur majeur pour les services agyw
def tranche_age_classique(age):
        if age>=10 and age<=14:
            return "10-14"
        elif age>=15 and age<=19:
            return "15-19"
        elif age>=20 and age<=24:
            return "20-24"
        elif age>=25 and age<=29:
            return "25-29"
        else:
            return "not_valid_age"
        
def tranche_age_mineur_majeur(age):
        if age>=10 and age<=17:
            return "10-17"
        elif age>=18 and age<=19:
            return "18-19"
        elif age>=20 and age<=24:
            return "20-24"
        elif age>=25 and age<=29:
            return "25-29"
        else:
            return "not_valid_age"


def fiscalYear21(date):
    if date.year == 2021 and date.month>=1 and date.month<=3:
        return "FY21Q2"
    elif date.year == 2020 and date.month>=10 and date.month<=12:
        return "FY21Q1"
    elif date.year == 2021 and date.month>=4 and date.month<=6:
        return "FY21Q3"
    elif date.year == 2021 and date.month>=7 and date.month<=9:
        return "FY21Q4"
    else:
        return "not_valid_fy"
    

    

def validTimeOnSystem(date):
    if date>= datetime.strptime("2020-04-01","%Y-%m-%d") and date<= datetime.now():
        return "required_Time_on"
    else:
        return "not_valid_time_on"
        
    

    
def between_now_date_entevyou(date):
    return (datetime.now().year - date.year) * 12 + (datetime.now().month - date.month)


def agywPeriods(months):
    if months <= 6:
        return "0-6 months"
    elif months>=7 and months<=12:
        return "07-12 months"
    elif months>=13 and months<=24:
        return "13-24 months"
    else:
        return "25+ months"

def curriculum_atLeastOneService(topics):
    return "servis_auMoins_1fois" if topics>=1 and topics<=18 else "zero_services_curriculum"


def status_curriculum(topics):
    if topics>=1 and topics<=13:
        return "curriculum incomplet"
    elif topics>=14 and topics<=18:
        return "curriculum complet"
    else:
        return "non-recu"

def curriculum_condense(curriculum):
    return "curriculum_completed" if curriculum == "curriculum complet" else "curriculum_inc"

def id_quarter_services(date):
    if type(date) == type(NaT):
        return 'errata'
    if (type(date) != type(NaT)) and (date.year == 2021 and date.month>=1 and date.month<=3):
        return "FY21Q2"
    elif (type(date) != type(NaT)) and (date.year == 2020 and date.month>=10 and date.month<=12):
        return "FY21Q1"
    elif (type(date) != type(NaT)) and (date.year == 2021 and date.month>=4 and date.month<=6):
        return "FY21Q3"
    elif (type(date) != type(NaT)) and (date.year == 2021 and date.month>=7 and date.month<=9):
        return "FY21Q4"
    elif (type(date) != type(NaT)) and (date.year == 2020 and date.month>=1 and date.month<=3):
        return "FY20Q2"
    elif (type(date) != type(NaT)) and (date.year == 2019 and date.month>=10 and date.month<=12):
        return "FY20Q1"
    elif (type(date) != type(NaT)) and (date.year == 2020 and date.month>=4 and date.month<=6):
        return "FY20Q3"
    elif (type(date) != type(NaT)) and (date.year == 2020 and date.month>=7 and date.month<=9):
        return "FY20Q4"
    elif (type(date) != type(NaT)) and (date.year == 2019 and date.month>=1 and date.month<=3):
        return "FY19Q2"
    elif (type(date) != type(NaT)) and (date.year == 2018 and date.month>=10 and date.month<=12):
        return "FY19Q1"
    elif (type(date) != type(NaT)) and (date.year == 2019 and date.month>=4 and date.month<=6):
        return "FY19Q3"
    elif (type(date) != type(NaT)) and (date.year == 2019 and date.month>=7 and date.month<=9):
        return "FY19Q4"
    else:
        return "not_valid_fy"


def hcvg_valid_services(date):
    if type(date) == type(NaT):
        return 'errata'
    elif (type(date) != type(NaT))and(date.year==2020 or date.year==2021):
        return 'tested_on_given_date'
    else:
        return 'not_valid_date'


def post_care_app(df):
    return 'service_gyneco_vbg' if (df.vbg=="tested_on_given_date") or (df.gyneco=='tested_on_given_date') else 'no'


def socioEco_app(df):
    return 'service_muso_gardening' if (df.is_muso == 'yes') or (df.is_gardening == 'yes') else 'no'


def unServiceDreams(df):
     return '1_services_dreams_recus' if (df.curriculum_servis_auMoins_1fois == "servis_auMoins_1fois") or (df.condoms=='tested_on_given_date') or (df.hts=='tested_on_given_date') or (df.post_care_treatment=="service_gyneco_vbg") or (df.socio_eco_app=="service_muso_gardening") else 'no'



def service_primaire_10_14(df):
    return 'curriculum-servis' if (df.curriculum=="curriculum complet" and df.age_range == "10-14") else "no"

def service_primaire_15_19(df):
    return 'condoms&curriculum' if (df.curriculum=="curriculum complet" and df.age_range == "15-19" and df.condoms=='tested_on_given_date') else 'no'

def service_primaire_20_24(df):
    return 'condoms&hts&curriculum' if (df.curriculum=="curriculum complet" and df.condoms=='tested_on_given_date' and df.hts=='tested_on_given_date' and df.age_range == "20-24") else 'no'


def isAGYW(total):
    return 'eligible' if total>=14 else 'no_eligible'


def new_service_primaire_20_24(df):
    return 'condoms&curriculum' if (df.curriculum=="curriculum complet" and df.condoms=='tested_on_given_date' and df.age_range == "20-24") else 'no'
