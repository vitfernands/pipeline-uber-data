import pandas as pd

def reading_csv(path):
    return pd.read_csv(path)
   
def treating_nan_values(data):
    data['Avg CTAT'] = data['Avg CTAT'].fillna(0)
    data['Avg VTAT'] = data['Avg CTAT'].fillna(0)
    data['Cancelled Rides by Customer'] = data['Cancelled Rides by Customer'].fillna(0)
    data['Cancelled Rides by Driver'] = data['Cancelled Rides by Driver'].fillna(0)
    data['Incomplete Rides'] = data['Incomplete Rides'].fillna(0)

    return data

def filling_nan_values(data, columns:list, value):
    for column in columns:
        data[column] = data[column].fillna(value)

    return data

def avg_waiting_time(data):
    data['AVG Waiting Time'] = (data['Avg CTAT'] + data['Avg VTAT']).round(2)
    return data

def transforming_todatetime(data, columns:list):
    for column in columns:
        data[column] = pd.to_datetime(data[column])
    
    return data

def float_into_int(data, columns:list):
    for column in columns:
        data[column] = data[column].astype(int)

    return data

def saving_data(data, path, format):
    if format.lower() == 'csv':
        data.to_csv(path)
    if format.lower() == 'parquet':
        data.to_parquet(path, engine="pyarrow", index=False)

