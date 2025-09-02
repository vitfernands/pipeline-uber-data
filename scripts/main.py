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

data_datetime = transforming_todatetime(data_avg_waiting_time, ['Date'])

data_transformed = float_into_int(data_datetime, [
    'Cancelled Rides by Customer',
    'Cancelled Rides by Driver',
    'Incomplete Rides'
])

print(data_transformed)

saving_data(data_transformed, 'data-processed/uber_data.csv')