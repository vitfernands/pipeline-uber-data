from cleaning_data import *

path = 'data-raw/ncr_ride_bookings.csv'

data = reading_csv(path)

data_nan_val = filling_nan_values(data,[
    'Avg CTAT',
    'Avg VTAT',
    'Cancelled Rides by Customer',
    'Cancelled Rides by Driver',
    'Incomplete Rides'
]
,0)

data_avg_waiting_time = avg_waiting_time(data_nan_val)

#data_datetime = transforming_todatetime(data_avg_waiting_time, ['Date'])

data_datetime = concat_columns(data_avg_waiting_time, ['Date', 'Time'], 'Datetime')

data_drop = drop_columns(data_datetime, ['Date', 'Time'])

print(data_drop)

data_transformed = float_into_int(data_drop, [
    'Cancelled Rides by Customer',
    'Cancelled Rides by Driver',
    'Incomplete Rides'
])

#print(data_transformed)

saving_data(data_transformed, 'data-processed/uber_data.csv', 'csv')
saving_data(data_transformed, 'data-processed/uber_data.parquet', 'parquet')