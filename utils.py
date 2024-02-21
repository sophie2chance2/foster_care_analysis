def var_values(variable_values_df, var_name):
    var_value_df = variable_values_df[variable_values_df['VarName'] == var_name]
    var_value = dict(zip(var_value_df['Value'], var_value_df['ValueLabel']))
    
    return var_value