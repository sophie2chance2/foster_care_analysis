import pandas as pd
import numpy as np
from io import BytesIO
import re

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

def map_var_values(variable_values_df, df):
    date_cols = ['DOB', 'Rem1Dt', 'DLstFCDt', 'LatRemDt', 'CurSetDt', 'DoDFCDt', 'TPRMomDt', 'TPRDadDt', 'PedRevDt', 'RemTrnDt', 'DoDTrnDt', 'TPRDate']
    race_columns = ['AMIAKN', 'ASIAN', 'BLKAFRAM', 'HAWAIIPI', 'WHITE', 'UNTODETM', 'HISORGIN']
    caretaker_race_columns = ['RF1AMAKN', 'RF1ASIAN', 'RF1BLKAA', 'RF1NHOPI', 'RF1WHITE', 'RF1UTOD', 'HOFCCTK1', 'RF2AMAKN', 'RF2ASIAN', 'RF2BLKAA', 'RF2NHOPI', 'RF2WHITE', 'RF2UTOD', 'HOFCCTK2']
    drop_columns = ['Version', # all values are 5
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
    df['DataYear'] = df['DataYear'].astype(int)

    for col in df.columns:
        if col in variable_values_df['VarName'].unique():
            df[col] = df[col].map(var_values(variable_values_df, col))
        elif (col in date_cols) or (col in race_columns) or (col in caretaker_race_columns or (col in drop_columns)):
            # drop all date columns
            df.drop(columns=[col], inplace=True)
        else:
            print(f"{col} not in variable_values_df")
    return df

def make_readable(year_df, variable_values_df):
    readable_df = year_df.reset_index(drop=True)
    readable_df['DataYear'] = readable_df['DataYear'].astype(int)

    # Make column names uppercase for consistency across years
    readable_df.columns = readable_df.columns.str.upper()

    ## Commenting out recnumbr changes since we filter it out anyway
    # # if RecNumbr is a byte string, decode it to a regular string
    # if isinstance(year_df['RecNumbr'][0], int):
    #     readable_df['RecNumbr'] = readable_df['RecNumbr'].astype(str)
    # else:
    #     readable_df['RecNumbr'] = readable_df['RecNumbr'].str.decode('latin1')
    #     readable_df['RecNumbr'] = readable_df['RecNumbr'].str.lstrip('0')

    readable_df['State'] = readable_df['STATE'].map(var_values(variable_values_df, 'State'))
    variable_values_df['VarName'] = variable_values_df['VarName'].replace({'FIPSCODE': 'FIPSCode'})
    readable_df['FIPSCode'] = readable_df['FIPSCODE'].map(var_values(variable_values_df, 'FIPSCode'))
    readable_df['Sex'] = readable_df['SEX'].map(var_values(variable_values_df, 'SEX'))
    readable_df['AgeAdopt'] = readable_df['AGEADOPT'].map(var_values(variable_values_df, 'AGEADOPT'))
    readable_df['removalManner'] = readable_df['MANREM'].map(var_values(variable_values_df, 'MANREM'))
    readable_df['currentPlacementSetting'] = readable_df['CURPLSET'].map(var_values(variable_values_df, 'CURPLSET'))
    readable_df['OutOfStatePlacement'] = readable_df['PLACEOUT'].map(var_values(variable_values_df, 'PLACEOUT'))
    readable_df['caseGoal'] = readable_df['CASEGOAL'].map(var_values(variable_values_df, 'CASEGOAL'))
    readable_df['caretakerFamilyStructure'] = readable_df['CTKFAMST'].map(var_values(variable_values_df, 'CTKFAMST'))
    readable_df['fosterFamilyStructure'] = readable_df['FOSFAMST'].map(var_values(variable_values_df, 'FOSFAMST'))
    readable_df['dischargeReason'] = readable_df['DISREASN'].map(var_values(variable_values_df, 'DISREASN'))
    readable_df['raceEthnicity'] = readable_df['RACEETHN'].map(var_values(variable_values_df, 'RaceEthn'))
    readable_df['everAdopted'] = readable_df['EVERADPT'].map(var_values(variable_values_df, 'EVERADPT'))
    readable_df['diagnosedDisability'] = readable_df['CLINDIS'].map(var_values(variable_values_df, 'CLINDIS'))

    # readable_df['firstCaretakerAge'] = readable_df['DATAYEAR'] - readable_df['CTK1YR']
    # readable_df['secondCaretakerAge'] = readable_df['DATAYEAR'] - readable_df['CTK2YR']

    # readable_df['firstFosterCaretakerAge'] = readable_df['DATAYEAR'] - readable_df['FCCTK1YR']
    # readable_df['secondFosterCaretakerAge'] = readable_df['DATAYEAR'] - readable_df['FCCTK2YR']

    readable_df['DOB'] = pd.to_datetime(readable_df['DOB'], errors='coerce')
    readable_df['age2021'] = pd.to_datetime('2021-12-31') - readable_df['DOB']
    readable_df['age2021'] = readable_df['age2021'].dt.days // 365

    # TODO orgaize the date columns: DOB, Rem1Dt, DLstFCDt, LatRemDt, CurSetDt, DoDFCDt, TPRMomDt, TPRDadDt, PedRevDt, RemTrnDt, DoDTrnDt

    # date_cols = ['DOB', 'Rem1Dt', 'DLstFCDt', 'LatRemDt', 'CurSetDt', 'DoDFCDt', 'TPRMomDt', 'TPRDadDt', 'PedRevDt', 'RemTrnDt', 'DoDTrnDt', 'TPRDate'] # using upper_date_cols instead
    upper_date_cols = ['DOB', 'REM1DT', 'DLSTFCDT', 'LATREMDT', 'CURSETDT', 'DODFCDT', 'TPRMOMDT', 'TPRDADDT', 'PEDREVDT','REMTRNDT', 'DODTRNDT', 'TPRDATE']
    race_columns = ['AMIAKN', 'ASIAN', 'BLKAFRAM', 'HAWAIIPI', 'WHITE', 'UNTODETM', 'HISORGIN']
    caretaker_race_columns = ['RF1AMAKN', 'RF1ASIAN', 'RF1BLKAA', 'RF1NHOPI', 'RF1WHITE', 'RF1UTOD', 'HOFCCTK1', 'RF2AMAKN', 'RF2ASIAN', 'RF2BLKAA', 'RF2NHOPI', 'RF2WHITE', 'RF2UTOD', 'HOFCCTK2']

    readable_df.drop(columns=['VERSION', # all values are 5
                        'STATE', 'ST', # Duplicitive of State column
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
                        'RACEETHN', # In column 'raceEthnicity' with mapped values
                        'EVERADPT', # In column 'everAdopted' with mapped values
                        'CLINDIS', # In column 'diagnosedDisability' with mapped values
                        'STFCID', # Repeat of RecNumbr
                        'AGEATSTART', # Repeat of Age at end
                        'ENTERED', # Repeat of INATSTART
                        'RACE', # Repeat of raceEthnicity
                        'LATREMLOS', # column seems repetitive in nature
                        'RU13', # No mapping but would like to add back in # TODO
                        ]
                        + race_columns # These are in the 'raceEthnicity' column
                        + caretaker_race_columns # TODO See if we want to add these back in?
                        + upper_date_cols # TODO See if we want to add these back in?
                        , inplace=True)
    return readable_df

def remove_nan_values(all_records):
    '''Clean up NaNs'''
    # Payment Columns
    all_records['FCMNTPAY'] = all_records['FCMNTPAY'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid
    all_records['IVEFC'] = all_records['IVEFC'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid
    all_records['IVAAFDC'] = all_records['IVAAFDC'].replace(np.nan, 0).astype(float) # Assumed that if null then they are not getting paid

    # Length of Stay Columns
    all_records['PREVIOUSLOS'] = all_records['PREVIOUSLOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0 because it means they have no previous LOS
    all_records['LIFELOS'] = all_records['LIFELOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0, if this is null, it is their first placement
    all_records['SETTINGLOS'] = all_records['SETTINGLOS'].replace(np.nan, 0).astype(float) # Replace NaNs with 0 # TODO: Not sure if this is an accurate represnetation, need to look into this more

    # Placement
    all_records['TOTALREM'] = all_records['TOTALREM'].replace(np.nan, 1).astype(float) # Assume first removal, all records with null have a value of 0 in PreviousLOS
    all_records['NUMPLEP'] = all_records['NUMPLEP'].replace(np.nan, 1).astype(float) # Assume 1 placement if null

    # Default to no for these columns # TODO - Decide if this is the best way to handle these
    nan_to_0_cols = ['VISHEAR', 'PHYDIS', 'MR', 'OTHERMED', 'RELINQSH', 'HOUSING', 'PRTSDIED', 'PRTSJAIL', 'CHILDIS', 'DACHILD', 'AACHILD', 'ABANDMNT', 'CHBEHPRB', 'NOCOPE', 'DAPARENT', 'AAPARENT', 'SEXABUSE', 'PHYABUSE', 'IVDCHSUP', 'NOA', 'IVEAA', 'XIXMEDCD', 'IVAAFDC', 'NEGLECT', 'SSIOTHER', 'EMOTDIST'] # 'DSMIII', 
    all_records[nan_to_0_cols] = all_records[nan_to_0_cols].fillna(0)

    # Replace NaNs with 'DNG' (Data Not Given) for these columns - seperate from Unknown
    nan_to_dng_cols = ['currentPlacementSetting', 'dischargeReason', 'fosterFamilyStructure', 'everAdopted', 'caretakerFamilyStructure', 'diagnosedDisability', 'OutOfStatePlacement', 'removalManner', 'caseGoal', 'FIPSCode', 'Sex', 'AgeAdopt']
    # all_records[nan_to_dng_cols] = all_records[nan_to_dng_cols].fillna('DNG')
    all_records.loc[:, nan_to_dng_cols] = all_records.loc[:, nan_to_dng_cols].fillna('DNG')

    # # Drop any rows where the id did not come in properly # TODO: Investigate why this is happening
    # print(f"Records with a bad ID: {len(all_records[all_records['RecNumbr'].str.contains('[a-zA-Z]', na=False)])}")
    # all_records = all_records[~all_records['RecNumbr'].str.contains('[a-zA-Z]', na=False)]

    # # Drop records where RecNumbr is unknown
    # all_records = all_records[all_records['RecNumbr'] == all_records['RecNumbr']] # Drop records where RecNumbr is unknown
    # print(f"RecNumbr Unknown: {len(all_records[all_records['RecNumbr'] != all_records['RecNumbr']])}")

    # # Drop records where age is unknown
    # all_records['AGEATSTART'] = all_records['AGEATSTART'].fillna(99) # some are blank and some are 99, entry error
    # all_records = all_records[all_records['AGEATSTART'] != 99]
    # print(f"Age Unknown: {len(all_records[all_records['AGEATSTART'] == 99])}\n")

    # Final Stats
    print(f"Total Null Values: {all_records.isnull().sum().sum()}")
    print(f"Total Records: {len(all_records)}")
    print(f"Total Columns: {len(all_records.columns)}")

    return all_records

def mark_reentries(df, start_year, end_year):
    # Assuming year columns are string type, if not convert them
    df.columns = df.columns.map(str)
    year_columns = [str(year) for year in range(start_year, end_year + 1)]

    # Initialize a flag column to False
    df['Reentry'] = False

    # Regular expression to match a '1' followed by any number of '0's followed by a '1'
    reentry_pattern = re.compile('10+1')

    # Iterate over rows to detect re-entry pattern
    for index, row in df.iterrows():
        # Extract the row values for the year range into a list, ignoring NaNs
        year_values = row[year_columns].tolist()

        # Create a string representation of the row's year values
        pattern = ''.join(['1' if x == 1.0 else '0' for x in year_values])

        # Use the regular expression to search for the pattern
        if reentry_pattern.search(pattern):
            df.at[index, 'Reentry'] = True
    
    return df
