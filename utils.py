import pandas as pd

def var_values(variable_values_df, var_name):
    var_value_df = variable_values_df[variable_values_df['VarName'] == var_name]
    var_value = dict(zip(var_value_df['Value'], var_value_df['ValueLabel']))
    
    return var_value

def make_readable(year_df, variable_values_df):
    readable_df = year_df.reset_index(drop=True)
    readable_df['DataYear'] = readable_df['DataYear'].astype(int)
    # if RecNumbr is a byte string, decode it to a regular string
    if isinstance(year_df['RecNumbr'][0], int):
        readable_df['RecNumbr'] = readable_df['RecNumbr'].astype(str)
    else:
        readable_df['RecNumbr'] = readable_df['RecNumbr'].str.decode('latin1')
        readable_df['RecNumbr'] = readable_df['RecNumbr'].str.lstrip('0')

    readable_df['State'] = readable_df['STATE'].map(var_values(variable_values_df, 'State'))

    variable_values_df['VarName'] = variable_values_df['VarName'].replace({'FIPSCODE': 'FIPSCode'})
    readable_df['FIPSCode'] = readable_df['FIPSCODE'].map(var_values(variable_values_df, 'FIPSCode'))

    readable_df['Sex'] = readable_df['SEX'].map(var_values(variable_values_df, 'SEX'))

    readable_df['AgeAdopt'] = readable_df['AGEADOPT'].map(var_values(variable_values_df, 'AGEADOPT')).fillna('Unknown') # TODO Look into this more, why are some blank and some 'unable to determine'

    readable_df['removalManner'] = readable_df['MANREM'].map(var_values(variable_values_df, 'MANREM'))
    readable_df['currentPlacementSetting'] = readable_df['CURPLSET'].map(var_values(variable_values_df, 'CURPLSET'))
    readable_df['OutOfStatePlacement'] = readable_df['PLACEOUT'].map(var_values(variable_values_df, 'PLACEOUT'))
    readable_df['caseGoal'] = readable_df['CASEGOAL'].map(var_values(variable_values_df, 'CASEGOAL'))
    readable_df['caretakerFamilyStructure'] = readable_df['CTKFAMST'].map(var_values(variable_values_df, 'CTKFAMST'))
    readable_df['fosterFamilyStructure'] = readable_df['FOSFAMST'].map(var_values(variable_values_df, 'FOSFAMST'))
    readable_df['dischargeReason'] = readable_df['DISREASN'].map(var_values(variable_values_df, 'DISREASN'))
    readable_df['raceEthnicity'] = readable_df['RaceEthn'].map(var_values(variable_values_df, 'RaceEthn'))
    readable_df['everAdopted'] = readable_df['EVERADPT'].map(var_values(variable_values_df, 'EVERADPT'))

    readable_df['firstCaretakerAge'] = readable_df['DataYear'] - readable_df['CTK1YR']
    readable_df['secondCaretakerAge'] = readable_df['DataYear'] - readable_df['CTK2YR']

    readable_df['firstFosterCaretakerAge'] = readable_df['DataYear'] - readable_df['FCCTK1YR']
    readable_df['secondFosterCaretakerAge'] = readable_df['DataYear'] - readable_df['FCCTK2YR']

    readable_df['DOB'] = pd.to_datetime(readable_df['DOB'], errors='coerce')
    readable_df['age2021'] = pd.to_datetime('2021-12-31') - readable_df['DOB']
    readable_df['age2021'] = readable_df['age2021'].dt.days // 365

    # TODO orgaize the date columns: DOB, Rem1Dt, DLstFCDt, LatRemDt, CurSetDt, DoDFCDt, TPRMomDt, TPRDadDt, PedRevDt, RemTrnDt, DoDTrnDt

    date_cols = ['DOB', 'Rem1Dt', 'DLstFCDt', 'LatRemDt', 'CurSetDt', 'DoDFCDt', 'TPRMomDt', 'TPRDadDt', 'PedRevDt', 'RemTrnDt', 'DoDTrnDt', 'TPRDate']

    race_columns = ['AMIAKN', 'ASIAN', 'BLKAFRAM', 'HAWAIIPI', 'WHITE', 'UNTODETM', 'HISORGIN']
    caretaker_race_columns = ['RF1AMAKN', 'RF1ASIAN', 'RF1BLKAA', 'RF1NHOPI', 'RF1WHITE', 'RF1UTOD', 'HOFCCTK1', 'RF2AMAKN', 'RF2ASIAN', 'RF2BLKAA', 'RF2NHOPI', 'RF2WHITE', 'RF2UTOD', 'HOFCCTK2']

    readable_df.drop(columns=['Version', # all values are 5
                        'STATE', 'St', # Duplicitive of State column
                        'REPDATYR', # Not necessary - Reporting end year - all values are 2001
                        'REPDATMO', # Not necessary - Reporting end month - all values are either 9 or 3
                        'FIPSCODE', # Not necessary - FIPS code is in a separate column
                        'SEX', # In column 'Sex' with mapped values
                        'AGEADOPT', # In column 'AgeAdopt' with mapped values
                        'MANREM', # In column 'removalManner' with mapped values
                        'CURPLSET', # In column 'currentPlacementSetting' with mapped values
                        'PLACEOUT', # In column 'OutOfStatePlacement' with mapped values
                        'CASEGOAL', # In column 'caseGoal' with mapped values
                        'CTKFAMST', # In column 'caretakerFamilyStructure' with mapped values
                        'CTK1YR', # In column 'firstCaretakerAge' with age values
                        'CTK2YR', # In column 'secondCaretakerAge' with age values
                        'FOSFAMST', # In column 'fosterFamilyStructure' with mapped values
                        'FCCTK1YR', # In column 'firstFosterCaretakerAge' with age values
                        'FCCTK2YR', # In column 'secondFosterCaretakerAge' with age values
                        'DISREASN', # In column 'dischargeReason' with mapped values
                        'RaceEthn', # In column 'raceEthnicity' with mapped values
                        'EVERADPT', # In column 'everAdopted' with mapped values
                        ]
                        + race_columns # These are in the 'raceEthnicity' column
                        + caretaker_race_columns # TODO See if we want to add these back in?
                        + date_cols # TODO See if we want to add these back in?
                        , inplace=True)
    return readable_df
