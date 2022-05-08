import pandas as pd
import os

station_df = pd.read_fwf('./isd-history.txt')                               # update location to station identification file
country_df = pd.read_fwf('/content/country-list.txt')                       # update location to country list file

for csv_file in sorted(os.listdir('./drive/MyDrive/STAT 480/Data')):        # update location to drive folder
  print(f"Working on -> {csv_file[csv_file.rfind('/') + 1:]}")
  year_data = pd.read_csv('./drive/MyDrive/STAT 480/Data/2000weatherdata.csv').drop(columns='Unnamed: 0')
  
  # unlawful merger if not performed
  year_data['station_num'] = year_data['station_num'].astype('str')

  # gather station information per merger
  merged_df = pd.merge(
    year_data,
    station_df,
    right_on='USAF',
    left_on='station_num'
  )[['station_num', 'temp_ft', 'dewpt_ft', 'slp_mb', 'visib_mi', 'wind_knt',
        'maxwind_knt', 'max_gust_knt', 'year', 'month', 'day', 'fog', 'rain',
        'snow', 'hail', 'thunder', 'tornado', 'precip_in', 'precip_flag',
        'max_temp_frnht', 'min_temp_frnht',
        'CTRY', 'ST', 'LAT', 'LON', 'ELEV(M)']]
  merged_df.columns = merged_df.columns.str.lower()

  # merge again to gather name of country
  fin_df = pd.merge(
    merged_df,
    country_df,
    left_on='ctry',
    right_on='FIPS'
  ).drop(columns=['FIPS', 'ID'])
  fin_df.columns = fin_df.columns.str.lower()

  # writing since Colab doesn't like RAM
  fin_df.to_csv(csv_file[:csv_file.rfind('.csv')] + '_updated.csv')
