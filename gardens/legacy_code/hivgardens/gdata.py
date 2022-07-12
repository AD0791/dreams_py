from pandas import read_excel

jardins = read_excel('../gdata/dreamsMars.xlsx')
jardins.fillna('---',inplace=True)
jardins['code'] = jardins.code_dreams.str.fullmatch("^[A-Z]{3}/DRMS/(\d{9})$")
jardins_code = jardins[jardins.code==True]
jardin = jardins_code[
    [
        'info.case_id',
        'code_dreams',
        'code',
        'closed',
        'closed_date',
        'start_date',
        'info.last_modified_date',
        'address_commune',
        'address_department',
        'cycle_number',
        'cycle_1_start_date',
        'gps',
        'beneficiary_type'
    ]
]

jardin_clean = jardin.drop_duplicates(subset=['code_dreams'])
