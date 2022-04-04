from pandas import DataFrame
from active import actif_served as AGYW_ACTIF


class AgywPrev:
    """A class with properties and methods given the results of the indicator AGYW_PREV DATIM"""
    __who_am_I = "DATIM"
    __datim1_title = "Number of individual AGYW that have fully completed the entire DREAMS primary package of services/interventions but no additional services/interventions."
    __datim2_title = "Number of individual AGYW that have fully completed the entire DREAMS primary package of services/interventions AND at least one secondary service/intervention."
    __datim3_title = "Number of individual AGYW that have completed at least one DREAMS service/intervention but not the full primary package."
    __datim4_title = "Number of AGYW that have started a DREAMS service/intervention but have not yet completed it."

    def __repr__(self):
        return f"<AgywPrev {self.__i_am}>"

    def __str__(self):
        return f"<AgywPrev {self.__i_am}>"

    @classmethod
    def datim_titleI(cls):
        return cls.__datim1_title

    @classmethod
    def datim_titleII(cls):
        return cls.__datim2_title

    @classmethod
    def datim_titleIII(cls):
        return cls.__datim3_title

    @classmethod
    def datim_titleIV(cls):
        return cls.__datim4_title

    def __init__(self, commune=None):
        self.__commune = commune
        self.__i_am = f"{AgywPrev.__who_am_I}"
        self.__data = AGYW_ACTIF
        self. __total_mastersheet = self.__data.id_patient.count()
        if self.__commune == None:
            self.__dreams_valid = self.__data[(self.__data.age_range != "not_valid_age") & (
                self.__data.age_range != "25-29")]
        else:
            self.__dreams_valid = self.__data[(self.__data.age_range != "not_valid_age") & (
                self.__data.age_range != "25-29") & (self.__data.commune == f"{self.__commune}")]
        self.__total_dreams_valid = self.__dreams_valid.id_patient.count()
        self.__dreams_valid["primary_only"] = self.__dreams_valid.apply(
            lambda df: self.__primFunc(df), axis=1)
        self.__dreams_valid["primary_and_OneSecondary_services"] = self.__dreams_valid.apply(
            lambda df: self.__primLeastOneSecFunc(df), axis=1)
        self.__dreams_valid["completed_one_service"] = self.__dreams_valid.apply(
            lambda df: self.__primPartFunc(df), axis=1)
        self.__dreams_valid["has_started_one_service"] = self.__dreams_valid.apply(
            lambda df: self.__hasStartedFunc(df), axis=1)
        self.__agyw_prevI = self.__dreams_valid[self.__dreams_valid.primary_only ==
                                                "full_primary_only"]
        self.__agyw_prevII = self.__dreams_valid[self.__dreams_valid.primary_and_OneSecondary_services ==
                                                 "full_primary_leastOneSecondary"]
        self.__agyw_prevIII = self.__dreams_valid[self.__dreams_valid.completed_one_service ==
                                                  "primary_part_services"]
        self.__agyw_prevIV = self.__dreams_valid[self.__dreams_valid.has_started_one_service == "yes"]
        self.__agyw_prevI_total = self.__agyw_prevI.id_patient.count()
        self.__agyw_prevII_total = self.__agyw_prevII.id_patient.count()
        self.__agyw_prevIII_total = self.__agyw_prevIII.id_patient.count()
        self.__agyw_prevIV_total = self.__agyw_prevIV.id_patient.count()
        self.__total_datim = self.__agyw_prevI_total + self.__agyw_prevII_total + \
            self.__agyw_prevIII_total + self.__agyw_prevIV_total

    @property
    def who_am_i(self):
        return self.__i_am

    @property
    def data_mastersheet(self):
        return self.__data

    @property
    def data_dreams_valid(self):
        return self.__dreams_valid

    @property
    def total_mastersheet(self):
        return self.__total_mastersheet

    @property
    def total_dreams_valid(self):
        return self.__total_dreams_valid

    def __primFunc(self, df):
        if (df.ps_1014 == "primary" and df.hts == "no" and df.prep == "no" and df.condom == "no" and df.post_violence_care == "no" and df.socioeco_app == "no" and df.parenting == "no" and df.contraceptive == "no"):
            return "full_primary_only"
        elif (df.ps_1519 == "primary" and df.hts == "no" and df.prep == "no" and df.post_violence_care == "no" and df.socioeco_app == "no" and df.parenting == "no" and df.contraceptive == "no"):
            return "full_primary_only"
        elif (df.ps_2024 == "primary" and df.hts == "no" and df.prep == "no" and df.post_violence_care == "no" and df.socioeco_app == "no" and df.parenting == "no" and df.contraceptive == "no"):
            return "full_primary_only"
        else:
            return "invalid"

    def __primLeastOneSecFunc(self, df):
        if (df.ps_1014 == "primary") and (df.hts == "yes" or df.prep == "yes" or df.condom == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes"):
            return "full_primary_leastOneSecondary"
        elif (df.ps_1519 == "primary") and (df.hts == "yes" or df.prep == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes"):
            return "full_primary_leastOneSecondary"
        elif (df.ps_2024 == "primary") and (df.hts == "yes" or df.prep == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes"):
            return "full_primary_leastOneSecondary"
        else:
            return "invalid"

    def __primPartFunc(self, df):
        if (df.age_range == "10-14") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and ((df.hts == "yes" or df.prep == "yes" or df.condom == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes")):
            return "primary_part_services"
        elif (df.age_range == "15-19") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and (df.curriculum == "yes" or df.condom == "yes" or df.hts == "yes" or df.prep == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes"):
            return "primary_part_services"
        elif (df.age_range == "20-24") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and ((df.curriculum == "yes" or df.condom == "yes" or df.hts == "yes" or df.prep == "yes" or df.post_violence_care == "yes" or df.socioeco_app == "yes" or df.parenting == "yes" or df.contraceptive == "yes")):
            return "primary_part_services"
        else:
            return "invalid"

    def __hasStartedFunc(self, df):
        if (df.age_range == "10-14") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and (df.completed_one_service == "invalid"):
            return "yes"
        elif (df.age_range == "15-19") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and (df.completed_one_service == "invalid"):
            return "yes"
        elif (df.age_range == "20-24") and (df.primary_only == "invalid") and (df.primary_and_OneSecondary_services == 'invalid') and (df.completed_one_service == "invalid"):
            return "yes"
        else:
            return "no"

    @property
    def total_datimI(self):
        return self.__agyw_prevI_total

    @property
    def total_datimII(self):
        return self.__agyw_prevII_total

    @property
    def total_datimIII(self):
        return self.__agyw_prevIII_total

    @property
    def total_datimIV(self):
        return self.__agyw_prevIV_total

    @property
    def total_datim_general(self):
        return self.__total_datim

    @property
    def data_agyw_prevI(self):
        return self.__agyw_prevI

    @property
    def data_agyw_prevII(self):
        return self.__agyw_prevII

    @property
    def data_agyw_prevIII(self):
        return self.__agyw_prevIII

    @property
    def data_agyw_prevIV(self):
        return self.__agyw_prevIV

    __PERIOD_DATIM = sorted(list(AGYW_ACTIF.month_in_program_range.unique()))
    __PERIOD_DATIM.append("Total")
    __AGE_DATIM = sorted(list(AGYW_ACTIF.age_range.unique())[0:3])

    def datim_agyw_prevI(self):

        try:
            pivotableI = self.__agyw_prevI.rename(
                columns={"age_range": "Age", "month_in_program_range": "Time"})
            agyw_prevI_pivot = pivotableI.pivot_table(index="Age", columns="Time", values="id_patient",
                                                      aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotI = list(agyw_prevI_pivot.columns)
            indexes_pivotI = list(agyw_prevI_pivot.index)
            for period in AgywPrev.__PERIOD_DATIM:
                if period not in columns_pivotI:
                    agyw_prevI_pivot[period] = 0
            for age in AgywPrev.__AGE_DATIM:
                if age not in indexes_pivotI:
                    agyw_prevI_pivot.loc[age] = 0
            agyw_prevI_pivot = agyw_prevI_pivot.reindex(
                index=AgywPrev.__AGE_DATIM, columns=AgywPrev.__PERIOD_DATIM)
            agyw_prevI_pivot_final = agyw_prevI_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevI_results_final = DataFrame(
                agyw_prevI_pivot_final.to_records(index=False))
        except ValueError:
            agyw_prevI_results_final = DataFrame({"Age": ["10-14", "15-19",
                                                          "20-24"],
                                                  "0-6 months": [0, 0, 0],
                                                  "07-12 months": [0, 0, 0],
                                                  "13-24 months": [0, 0, 0],
                                                  "25+ months": [0, 0, 0],
                                                  "Total": [0, 0, 0]
                                                  })
        return agyw_prevI_results_final

    def datim_agyw_prevII(self):
        try:
            pivotableII = self.__agyw_prevII.rename(
                columns={"age_range": "Age", "month_in_program_range": "Time"})
            agyw_prevII_pivot = pivotableII.pivot_table(
                index="Age", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotII = list(agyw_prevII_pivot.columns)
            indexes_pivotII = list(agyw_prevII_pivot.index)
            for period in AgywPrev.__PERIOD_DATIM:
                if period not in columns_pivotII:
                    agyw_prevII_pivot[period] = 0
            for age in AgywPrev.__AGE_DATIM:
                if age not in indexes_pivotII:
                    agyw_prevII_pivot.loc[age] = 0
            agyw_prevII_pivot = agyw_prevII_pivot.reindex(
                index=AgywPrev.__AGE_DATIM, columns=AgywPrev.__PERIOD_DATIM)
            agyw_prevII_pivot_final = agyw_prevII_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevII_results_final = DataFrame(
                agyw_prevII_pivot_final.to_records(index=False))
        except ValueError:
            agyw_prevII_results_final = DataFrame({"Age": ["10-14", "15-19",
                                                           "20-24"],
                                                   "0-6 months": [0, 0, 0],
                                                   "07-12 months": [0, 0, 0],
                                                   "13-24 months": [0, 0, 0],
                                                   "25+ months": [0, 0, 0],
                                                   "Total": [0, 0, 0]
                                                   })
        return agyw_prevII_results_final

    def datim_agyw_prevIII(self):
        try:
            pivotableIII = self.__agyw_prevIII.rename(
                columns={"age_range": "Age", "month_in_program_range": "Time"})
            agyw_prevIII_pivot = pivotableIII.pivot_table(
                index="Age", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotIII = list(agyw_prevIII_pivot.columns)
            indexes_pivotIII = list(agyw_prevIII_pivot.index)
            for period in AgywPrev.__PERIOD_DATIM:
                if period not in columns_pivotIII:
                    agyw_prevIII_pivot[period] = 0
            for age in AgywPrev.__AGE_DATIM:
                if age not in indexes_pivotIII:
                    agyw_prevIII_pivot.loc[age] = 0
            agyw_prevIII_pivot = agyw_prevIII_pivot.reindex(
                index=AgywPrev.__AGE_DATIM, columns=AgywPrev.__PERIOD_DATIM)
            agyw_prevIII_pivot_final = agyw_prevIII_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevIII_results_final = DataFrame(
                agyw_prevIII_pivot_final.to_records(index=False))
        except ValueError:
            agyw_prevIII_results_final = DataFrame({"Age": ["10-14",                     "15-19",
                                                            "20-24"],
                                                    "0-6 months": [0, 0, 0],
                                                    "07-12 months": [0, 0, 0],
                                                    "13-24 months": [0, 0, 0],
                                                    "25+ months": [0, 0, 0],
                                                    "Total": [0, 0, 0]
                                                    })
        return agyw_prevIII_results_final

    def datim_agyw_prevIV(self):
        try:
            pivotableIV = self.__agyw_prevIV.rename(
                columns={"age_range": "Age", "month_in_program_range": "Time"})
            agyw_prevIV_pivot = pivotableIV.pivot_table(
                index="Age", columns="Time", values="id_patient", aggfunc="count", fill_value=0, margins=True, margins_name="Total", dropna=False)[:-1]
            columns_pivotIII = list(agyw_prevIV_pivot.columns)
            indexes_pivotIII = list(agyw_prevIV_pivot.index)
            for period in AgywPrev.__PERIOD_DATIM:
                if period not in columns_pivotIII:
                    agyw_prevIV_pivot[period] = 0
            for age in AgywPrev.__AGE_DATIM:
                if age not in indexes_pivotIII:
                    agyw_prevIV_pivot.loc[age] = 0
            agyw_prevIV_pivot = agyw_prevIV_pivot.reindex(
                index=AgywPrev.__AGE_DATIM, columns=AgywPrev.__PERIOD_DATIM)
            agyw_prevIV_pivot_final = agyw_prevIV_pivot.reset_index().rename_axis(None, axis=1)
            agyw_prevIV_results_final = DataFrame(
                agyw_prevIV_pivot_final.to_records(index=False))
        except ValueError:
            agyw_prevIV_results_final = DataFrame({"Age": ["10-14",                     "15-19",
                                                           "20-24"],
                                                   "0-6 months": [0, 0, 0],
                                                   "07-12 months": [0, 0, 0],
                                                   "13-24 months": [0, 0, 0],
                                                   "25+ months": [0, 0, 0],
                                                   "Total": [0, 0, 0]
                                                   })
        return agyw_prevIV_results_final


class AgywPrevCommune(AgywPrev):
    """A class that extend AgywPrev with the purpose of the indicator AGYW_PREV DATIM by commune"""
    __who_am_I = "DATIM"

    def __init__(self, name):
        self.__name = name
        self.__i_am = f"{AgywPrevCommune.__who_am_I} {self.__name}"
        super().__init__(self.__name)

    @property
    def who_am_i(self):
        return self.__i_am

    def __repr__(self):
        return f"<AgywPrevCommune {self.__i_am}>"

    def __str__(self):
        return f"<AgywPrevCommune {self.__i_am}>"
