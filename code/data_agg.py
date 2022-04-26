import os
import tarfile
import random

import pandas as pd
from alive_progress import alive_bar

import gzip_pal as gp


def spew_out_year_directory(year):
    gsod_dir = f'archive/gsod_all_years'
    year_tar_dir = list(filter(lambda x: str(year) in x, os.listdir(gsod_dir)))[0]
    if tarfile.is_tarfile(f'{gsod_dir}/{year_tar_dir}'):
        with tarfile.open(f'{gsod_dir}/{year_tar_dir}') as tar:
            tar.extractall(path=f'./{year}_data')

def gather_year_directory_data(year, n_samples):
    
    spew_out_year_directory(year)
    assert os.path.isdir(f'./{year}_data'), f'./{year}_data not valid directory'
    
    df_dict = {}
    dir_files = os.listdir(f'./{year}_data')
    
    with alive_bar(n_samples, title=f'Working on {year} ...') as bar:
        for sample in random.sample(population=range(len(dir_files)), k=n_samples):
            try:
                gz_file = dir_files[sample]
                df_dict[gz_file[:gz_file.find('-')]] = gzip_to_dataframe(f'./{year}_data/{gz_file}')
                os.remove(f'./{year}_data/{gz_file}')
                bar()
            except:
                continue
    
    return df_dict


def write_to_parquet(year, n_samples):
    
    # gather dataframes for given year
    agg_df_dict = gather_year_directory_data(year, n_samples)

    # clean up directory
    for file in os.listdir(f'./{year}_data'):
      if file.endswith('.gz'):
        os.remove(f'./{year}_data/{file}')
    
    # write valid stations to text file for retrieval later on
    with open(f'./{year}_data/{year}stations.txt', 'w') as st_file:
        total_stations = 0
        for st in agg_df_dict.keys():
            total_stations += 1
            st_file.write(st + '\n')
    
    # write to concatenated dataframe to corresponding year folder
    pd.concat(
        agg_df_dict.values()
        ).to_parquet(f'./{year}_data/{year}weatherdata.parquet')

    # log results
    if os.path.isfile(f'./{year}_data/{year}stations.txt'):
        print(f'>> Total stations: {total_stations}/{n_samples}')
    if os.path.isfile(f'./{year}_data/{year}weatherdata.csv'):
        print(f'>> Success: {year}weatherdata.parquet written to ./{year}_data/')
    else:
        print(f'!! Failure: {year}weatherdata.parquet NOT written to ./{year}_data/')



if __name__ == '__main__':
    
    years = range(1975, 2020)
    n_samples = 10000
    
    for year in years:
        write_to_parquet(year, n_samples)
