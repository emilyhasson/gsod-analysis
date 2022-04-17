import os
import tarfile

import pandas as pd
from alive_progress import alive_bar

import gzip_pal as gp


def spew_out_year_directory(year):
    gsod_dir = f'archive/gsod_all_years'
    year_tar_dir = list(filter(lambda x: str(year) in x, os.listdir(gsod_dir)))[0]
    if tarfile.is_tarfile(f'{gsod_dir}/{year_tar_dir}'):
        with tarfile.open(f'{gsod_dir}/{year_tar_dir}') as tar:
            tar.extractall(path=f'./{year}_data')

def gather_year_directory_data(year):
    
    spew_out_year_directory(year)
    assert os.path.isdir(f'./{year}_data'), f'./{year}_data not valid directory'

    df_dict = {}
    with alive_bar(len(os.listdir(f'./{year}_data')[:100]), title=f'Working on {year}...') as bar:
        for gz_file in os.listdir(f'./{year}_data')[:100]:
            try:
                df_dict[gz_file[:gz_file.find('-')]] = gp.gzip_to_dataframe(f'./{year}_data/{gz_file}')
                bar()
            except:
                continue    
    
    return df_dict

def write_to_csv(year):
    
    # gather dataframes for given year
    agg_df_dict = gather_year_directory_data(year)
    
    # write valid stations to text file for retrieval later on
    with open(f'./{year}_data/{year}stations.txt', 'w') as st_file:
        total_stations = 0
        for st in agg_df_dict.keys():
            total_stations += 1
            st_file.write(st + '\n')
    
    # write to concatenated dataframe to corresponding year folder
    pd.concat(
        agg_df_dict.values()
        ).to_csv(f'./{year}_data/{year}weatherdata.csv')
    
    # log results
    if os.path.isfile(f'./{year}_data/{year}stations.txt'):
        print(f'>> Total stations: {total_stations}')
    if os.path.isfile(f'./{year}_data/{year}weatherdata.csv'):
        print(f'>> Success: {year}weatherdata.csv written to ./{year}_data/')
    else:
        print(f'!! Failure: {year}weatherdata.csv NOT written to ./{year}_data/')


if __name__ == '__main__':
    years = range(2000, 2020)
    for year in years:
        write_to_csv(year)