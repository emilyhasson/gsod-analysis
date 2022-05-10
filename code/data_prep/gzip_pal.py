import gzip

import pandas as pd
import numpy as np


def gzip_to_dataframe(gz_file):
    try:
        # read in file as fixed-width file (FWF)
        df = pd.read_fwf(gzip.open(gz_file))

        # focus on observations with recorded date
        df = df[df['YEARMODA'].notna()]

        # locate and filter out coded missing values (e.g. 9999, 99.99, 99999.9)
        df_without_year = df.drop(columns='YEARMODA').astype('str'
        ).replace(to_replace=r'[9]+[.]+[9]*', value=np.nan, regex=True
        ).dropna(axis=1, how='all')

        df = pd.concat([df_without_year, df['YEARMODA']], axis=1)

        # remove attributes with unnamed columns (these columns strictly count how many
        # observations were used when calculating the observed mean)
        df = df.drop(columns=df.columns[df.columns.str.startswith('Unnamed')]).fillna(0.0)

        # extract date information and save into separate columns
        df[['year', 'month', 'day']] = pd.to_datetime(df['YEARMODA'], format='%Y%m%d'
        ).astype('str').str.split('-', expand=True).astype('int64')

        if df['FRSHTT'].astype('int64').sum() != 0:
            df[['fog', 'rain', 'snow', 'hail', 'thunder', 'tornado']] = df['FRSHTT'].astype('int64'
            ).replace({0:'000000'}).astype(str).apply(list).apply(pd.Series
            ).fillna(0).astype(int)
        else:
            df[['fog', 'rain', 'snow', 'hail', 'thunder', 'tornado']] = np.zeros(shape=(df.shape[0], 6)).astype(int)

        # extract precipitation values and flags and store in respective columns
        df[['precip_in', 'precip_flag']] = df['PRCP'].str.extract('(?P<precip_in>[\d.]{4})(?P<precip_flag>[A-Z]{1})'
        ).fillna({'precip_in':0, 'flag':np.nan})

        df['precip_in'] = df['precip_in'].astype('float64')

        # replace pointless flags fro temperature columns
        df['max_temp_frnht'] = df['MAX'].apply(lambda x: str(x).replace('*', '')).astype('float64')
        df['min_temp_frnht'] = df['MIN'].apply(lambda x: str(x).replace('*', '')).astype('float64')

        # remove columns we just extracted data from and rename columns
        df.drop(columns=['FRSHTT', 'YEARMODA', 'MAX', 'MIN', 'PRCP', 'WBAN'], inplace=True)
        df.rename(columns={
            'STN---':'station_num',
            'TEMP':'temp_ft',
            'DEWP':'dewpt_ft',
            'SLP':'slp_mb',
            'VISIB':'visib_mi',
            'GUST':'max_gust_knt',
            'SNDP':'snow_depth_in',
            'WDSP':'wind_knt',
            'MXSPD':'maxwind_knt'
        }, inplace=True)

        # reorder columns and return dataframe
        beg_cols = ['station_num', 'year', 'month', 'day']
        end_cols = ['precip_in', 'precip_flag']
        mid_cols = list(df.columns[~df.columns.isin(beg_cols + end_cols)])

        return df#[beg_cols + mid_cols + end_cols]

    except:
      pass
