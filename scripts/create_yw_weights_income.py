import argparse
import csv
import os
import sys

import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# default parameters
DEF_DB = 'ctlb2'
DEF_USER_TABLE = 'income_yw_user_2020'
DEF_DEMOGRAPHICS = 'income'
DEF_SMOOTHING = 10
DEF_MIN_BIN_NUMBER = 0 # 50
dem_info = {
    'age': {
        'bins': [13, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 81],
        'sql_table': 'acs2015_5yr_age_gender_gte15',
        'sql_bins': [
            'total_15to19', 'total_20to24', 'total_25to29', 'total_30to34',
            'total_35to39', 'total_40to44', 'total_45to49', 'total_50to54',
            'total_55to59', 'total_60to64', 'total_65plus'
        ],
    },
    'income': {
        'bins': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        'sql_table': 'cnty_income_2019',
        'sql_bins': [
            'incomelt10k', 'income10kto14999', 'income15kto24999', 'income25kto34999',
            'income35kto49999', 'income50kto74999', 'income75kto99999',
            'income100kto149999', 'income150kto199999', 'incomegt200k'
        ],
    },
    'gender': {
        'bins': [0, 1, 5],
        'sql_table': 'acs2015_5yr_age_gender',
        'sql_bins': ['male_perc', 'female_perc'],
    },
    'education': {
        'bins': [0, 1, 5],
        'sql_table': 'acs2015_5yr_education', 
        'sql_bins': ['perc_high_school_or_higher', 'perc_bach_or_higher'],
    },
}
DEF_CENSUS_TABLE = dem_info[DEF_DEMOGRAPHICS]['sql_table']


def get_args():
    parser = argparse.ArgumentParser(description='post-stratification weights by county and week')
    parser.add_argument('--csv', action='store_true', help='use local csv files instead of mysql')
    parser.add_argument('--db', type=str, default=DEF_DB, help='mysql database')
    parser.add_argument('--user_table', default=DEF_USER_TABLE, help='user demographics mysql table or csv path')
    parser.add_argument('--census_table', default=DEF_CENSUS_TABLE, help='censuse demographics mysql table or csv path')
    parser.add_argument('--demographics', type=str, default=DEF_DEMOGRAPHICS, help='demographics')
    parser.add_argument('--smoothing', type=float, default=DEF_SMOOTHING, help='smoothing k constant')
    parser.add_argument('--min_bin_number', type=float, default=DEF_MIN_BIN_NUMBER, help='min_bin_number')
    parser.add_argument('--uninformed_smoothing', action='store_true', help='use uninformed smoothing')
    parser.add_argument('--smooth_before_binning', action='store_true', help='smooth before binning')
    parser.add_argument('--output', type=str, required=True, help='path for output weights csv')
    args = parser.parse_args()
    return args


def get_db_engine(db_schema, db_host='localhost', charset='utf8mb4', db_config='~/.my.cnf', port=3306):
    query = {'read_default_file' : db_config, 'charset': charset}
    db_url = URL(drivername='mysql', host=db_host, port=port, database=db_schema, query=query)
    eng = create_engine(db_url)
    return eng


def load_data_from_sql(db, user_table, census_table):
    engine = get_db_engine(db)
    user_sql = 'select user_id, cnty, year_week, income_redist_bin as income from {table}'.format(table=user_table)
    census_sql = 'select * from {table}'.format(table=census_table)
    user_df = pd.read_sql(user_sql, engine)
    census_df = pd.read_sql(census_sql, engine)
    return user_df, census_df


def load_data_from_csv(user_csv, census_csv):
    if '.csv' not in user_csv:
        user_csv += '.csv'
    if '.csv' not in census_csv:
        census_csv += '.csv'

    user_df = pd.read_csv(user_csv, header=0, usecols=['user_id', 'cnty', 'year_week', 'income_redist_bin'])
    user_df.rename(columns={'income_redist_bin': 'income'}, inplace=True)
    census_df = pd.read_csv(census_csv, header=0)
    return user_df, census_df


def load_data(use_csv, db, user_table, census_table):
    if use_csv:
        user_df, census_df = load_data_from_csv(user_table, census_table)
    else:
        user_df, census_df = load_data_from_sql(db, user_table, census_table)

    user_df.set_index(['cnty', 'year_week'], inplace=True)
    user_df.sort_index(inplace=True)
    census_df.set_index('cnty', inplace=True)
    census_df.sort_index(inplace=True)
    return user_df, census_df


def collapse_bins(bin_boundaries, bin_counts, smoothed_k=0, min_bin_number=0, smooth_before_binning=False, pop_percentages=[]):
    this_bin_boundaries = list(bin_boundaries)
    this_bin_counts = list(bin_counts)
    if smooth_before_binning:
        this_bin_counts = [bc + smoothed_k*pop_percentages[bc_idx]/100 for bc_idx, bc in enumerate(this_bin_counts)]
    while True:
        if len(this_bin_counts) <= 1 or all(p >= min_bin_number for p in this_bin_counts):
            break
        min_idx = this_bin_counts.index(min(this_bin_counts))
        del_idx = min_idx
        if min_idx == 0:
            min_adjacent_idx = min_idx + 1
            del_idx = min_idx + 1
        elif min_idx == len(this_bin_counts) - 1:
            min_adjacent_idx = min_idx - 1
        else:
            if this_bin_counts[min_idx-1] >= this_bin_counts[min_idx+1]: 
                min_adjacent_idx = min_idx+1
            else:
                min_adjacent_idx = min_idx-1
                
            if min_adjacent_idx >= min_idx:
                del_idx = min_idx + 1
                
        this_p = this_bin_counts[min_idx]
        this_bin = this_bin_boundaries[del_idx]
        this_bin_counts[min_adjacent_idx] += this_bin_counts[min_idx]
        
        del this_bin_counts[min_idx]
        del this_bin_boundaries[del_idx]
        if smooth_before_binning: 
            del pop_percentages[min_idx]
    return this_bin_boundaries, this_bin_counts


def get_twitter_bins(dem, df, smoothed_k=0, min_bin_number=0, smooth_before_binning=False, pop_percentages=[]):
    twitter_bins = dem_info[dem]['bins']
    values = df[dem].tolist()
    res = np.histogram(values, bins=twitter_bins)
    twitter_bins_counts = res[0]

    if min_bin_number > 0:
        twitter_bins, twitter_bins_counts = collapse_bins(twitter_bins, twitter_bins_counts, smoothed_k, min_bin_number, smooth_before_binning, pop_percentages)

    percentages = [x / len(values) for x in twitter_bins_counts]

    return twitter_bins, percentages


def get_population_bins(dem, df, twitter_bins):
    pop_bins = twitter_bins
    orig_bins = dem_info[dem]['bins']
    sql_bins = dem_info[dem]['sql_bins']

    if len(twitter_bins) < 3:
        return [100]

    if dem in ('gender', 'education'):
        return df[sql_bins].tolist()
    else:
        percentages = []
        base_idx = 0

        for i in pop_bins[1:-1]:
            this_idx = orig_bins.index(i)
            bin_total = np.sum(df[sql_bins[base_idx:this_idx]])
            percentages.append(bin_total)
            base_idx = this_idx

        percentages.append(100 - np.sum(percentages))
        return percentages


def create_weights(df, twitter_bins, twitter_percentages, pop_percentages, smoothed_k=0, dem='', uninformed_smoothing=False):
    total_twitter = df.shape[0]
    ii = len(twitter_bins) - 2
    total = 0
    if len(pop_percentages) == 1 and pop_percentages[0] == 100:
        df['weights'] = 1
        return df
    for bin_entry in twitter_bins[1:][::-1]:
        twitter_bin_n = round(twitter_percentages[ii] * total_twitter, 0)
        if not uninformed_smoothing:
            percentage_twitter_bin = (twitter_bin_n + smoothed_k * pop_percentages[ii] / 100) / (total_twitter + smoothed_k)
        else:
            percentage_twitter_bin = (twitter_bin_n + 1) / (total_twitter + len(twitter_percentages))
        total += twitter_bin_n
        if percentage_twitter_bin > 0:
            w = pop_percentages[ii] / (100 * percentage_twitter_bin)
        else:
            w = 0
        if ii == len(twitter_bins) - 2:
            df['weights'] = np.where(df[dem] < bin_entry, w, None)
        else:
            df['weights'] = np.where(df[dem] < bin_entry, w, df['weights'])
        ii -= 1
    return df


def main(args):
    print('CREATING WEIGHTS FOR:', args.demographics)
    print('     WITH SMOOTHING K =', args.smoothing)
    print('     WITH MIN BIN NUM =', args.min_bin_number)

    if args.uninformed_smoothing:
        print('     WITH UNINFORMED SMOOTHING')
    if args.smooth_before_binning:
        print('     WITH SMOOTH BEFORE BINNING')

    user_df, census_df = load_data(args.csv, args.db, args.user_table, args.census_table)
    cnty_list = user_df.index.unique().tolist()
    cnty_list.sort()
    print('Number of counties:', len(cnty_list))

    for counter, (cnty, week) in enumerate(cnty_list, start=1):
        print('Processing county: {cnty}, week: {week} [{count} / {total}]'.format(
            cnty=cnty,
            week=week,
            count=counter,
            total=len(cnty_list),
        ))

        try:
            census_subset = census_df.loc[cnty]
        except:
            print('    SKIPPING: CENSUS DOES NOT CONTAIN COUNTY', cnty)
            continue
        user_subset = user_df.loc[[(cnty, week)]]

        smoothed_k = args.smoothing
        if args.smooth_before_binning:
            twitter_bins, twitter_percentages = get_twitter_bins(args.demographics, user_subset)
            pop_percentages = get_population_bins(args.demographics, census_subset, twitter_bins)

            twitter_bins, twitter_percentages = get_twitter_bins(
                args.demographics, user_subset, smoothed_k=smoothed_k, min_bin_number=args.min_bin_number,
                smooth_before_binning=True, pop_percentages=pop_percentages
            )
            pop_percentages = get_population_bins(args.demographics, census_subset, twitter_bins)
            smoothed_k = 0
        else:
            twitter_bins, twitter_percentages = get_twitter_bins(args.demographics, user_subset, min_bin_number=args.min_bin_number)
            pop_percentages = get_population_bins(args.demographics, census_subset, twitter_bins)

        user_weights = create_weights(
            user_subset, twitter_bins, twitter_percentages, pop_percentages, smoothed_k, 
            args.demographics, uninformed_smoothing=args.uninformed_smoothing
        )

        with open(args.output, 'a') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            user_ids = user_weights['user_id'].tolist()
            weights  = user_weights['weights'].tolist()
            for i in range(len(user_ids)):
                writer.writerow([user_ids[i], week, cnty, weights[i]])


if __name__ == '__main__':
    args = get_args()
    main(args)
