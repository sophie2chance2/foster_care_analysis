import pandas as pd
import numpy as np
from io import BytesIO

def read_cloud_data(bucket, blob_name):
    bytes_stream = BytesIO()
    blob = bucket.blob(blob_name)
    blob.download_to_file(bytes_stream)
    bytes_stream.seek(0)  # Go to the beginning of the file-like object
    return bytes_stream

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
    readable_df['diagnosedDisability'] = readable_df['CLINDIS'].map(var_values(variable_values_df, 'CLINDIS'))


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
                        'CLINDIS', # In column 'diagnosedDisability' with mapped values
                        ]
                        + race_columns # These are in the 'raceEthnicity' column
                        + caretaker_race_columns # TODO See if we want to add these back in?
                        + date_cols # TODO See if we want to add these back in?
                        , inplace=True)
    return readable_df

def remove_nan_values(all_records):
    '''Clean up NaNs'''
    # Payment Columns
    all_records['FCMntPay'] = all_records['FCMntPay'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid
    all_records['IVEFC'] = all_records['IVEFC'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid
    all_records['IVAAFDC'] = all_records['IVAAFDC'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid

    # Length of Stay Columns
    all_records['PreviousLOS'] = all_records['PreviousLOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0 because it means they have no previous LOS
    all_records['LifeLOS'] = all_records['LifeLOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0, if this is null, it is their first placement
    all_records['SettingLOS'] = all_records['SettingLOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0 # TODO: Not sure if this is an accurate represnetation, need to look into this more

    # Placement
    all_records['TOTALREM'] = all_records['TOTALREM'].replace(np.nan, 1).astype(float) # Assume first removal, all records with null have a value of 0 in PreviousLOS
    all_records['NUMPLEP'] = all_records['NUMPLEP'].replace(np.nan, 1).astype(float) # Assume 1 placement if null

    # Default to no for these columns # TODO - Decide if this is the best way to handle these
    nan_to_0_cols = ['VISHEAR', 'PHYDIS', 'MR', 'OTHERMED', 'DSMIII', 'RELINQSH', 'HOUSING', 'PRTSDIED', 'PRTSJAIL', 'CHILDIS', 'DACHILD', 'AACHILD', 'ABANDMNT', 'CHBEHPRB', 'NOCOPE', 'DAPARENT', 'AAPARENT', 'SEXABUSE', 'PHYABUSE', 'IVDCHSUP', 'NOA', 'IVEAA', 'XIXMEDCD', 'IVAAFDC', 'NEGLECT', 'SSIOTHER']
    all_records[nan_to_0_cols] = all_records[nan_to_0_cols].fillna(0)

    # Replace NaNs with 'DNG' (Data Not Given) for these columns - seperate from Unknown
    nan_to_dng_cols = ['currentPlacementSetting', 'dischargeReason', 'fosterFamilyStructure', 'everAdopted', 'caretakerFamilyStructure', 'diagnosedDisability', 'OutOfStatePlacement', 'removalManner', 'caseGoal', 'FIPSCode', 'Sex']
    # all_records[nan_to_dng_cols] = all_records[nan_to_dng_cols].fillna('DNG')
    all_records.loc[:, nan_to_dng_cols] = all_records.loc[:, nan_to_dng_cols].fillna('DNG')

    # Drop any rows where the id did not come in properly # TODO: Investigate why this is happening
    print(f"Records with a bad ID: {len(all_records[all_records['RecNumbr'].str.contains('[a-zA-Z]', na=False)])}")
    all_records = all_records[~all_records['RecNumbr'].str.contains('[a-zA-Z]', na=False)]

    all_records = all_records.drop(columns=['secondCaretakerAge', 
                            'secondFosterCaretakerAge', 
                            'firstFosterCaretakerAge',
                            'firstCaretakerAge',
                            #   'RU13\r', # Not needed, Rural Urban Continuum Code, only included in 2002
                            #   'Race', # Not needed, incorperated into other columns (one hot encoding style), only included in 2002
                            'LatRemLOS', # column seems repetitive in nature
                            ])

    # Drop records where RecNumbr is unknown
    all_records = all_records[all_records['RecNumbr'] == all_records['RecNumbr']] # Drop records where RecNumbr is unknown
    print(f"RecNumbr Unknown: {len(all_records[all_records['RecNumbr'] != all_records['RecNumbr']])}")

    # Drop records where age is unknown
    all_records['AgeAtStart'] = all_records['AgeAtStart'].fillna(99) # some are blank and some are 99, entry error
    all_records = all_records[all_records['AgeAtStart'] != 99]
    print(f"Age Unknown: {len(all_records[all_records['AgeAtStart'] == 99])}\n")

    # Final Stats
    print(f"Total Null Values: {all_records.isnull().sum().sum()}")
    print(f"Total Records: {len(all_records)}")
    print(f"Total Columns: {len(all_records.columns)}")

    return all_records