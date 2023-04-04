import argparse

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


# default paramaters
DEFAULT_DB = 'ctlb2'
DEFAULT_TABLE = 'income_user_2020'
DEFAULT_PEW_BINS = [0, 30000, 50000, 75000]
DEFAULT_PEW_PERCS = [0.198889, 0.204738, 0.242761, 0.353612]


def get_args():
    parser = argparse.ArgumentParser(description='income redistribution script')
    parser.add_argument('--db', type=str, default=DEFAULT_DB, help='database')
    parser.add_argument('--table', type=str, default=DEFAULT_TABLE, help='user income table')
    parser.add_argument('--pew_bins', type=int, nargs=4, default=DEFAULT_PEW_BINS, help='PEW income bins')
    parser.add_argument('--pew_percs', type=float, nargs=4, default=DEFAULT_PEW_PERCS, help='PEW bin percentages')
    args = parser.parse_args()
    return args


def get_db_engine(db_schema, db_host='localhost', charset='utf8mb4', db_config='~/.my.cnf', port=3306):
    query = {'read_default_file' : db_config, 'charset': charset}
    db_url = URL(drivername='mysql', host=db_host, port=port, database=db_schema, query=query)
    eng = create_engine(db_url)
    return eng


def main(args):
    db = args.db
    table = args.table
    pew_bins = args.pew_bins.copy()
    pew_percs = args.pew_percs.copy()
    print('=== PARAMETERS ===')
    print('database:', db)
    print('table:', table)
    print('PEW bins:', pew_bins)
    print('PEW percentages:', pew_percs)

    engine = get_db_engine(db)

    with engine.connect() as conn:
        print('=== LOADING USER INCOME ===')
        query = 'select income from {table}'.format(table=table)
        result = conn.execute(query)
        income = np.array([r['income'] for r in result])
        total = len(income)
        min_income = np.min(income)
        max_income = np.ceil(np.max(income)) + 1
        print('total users:', total)
        print('min income:', min_income)
        print('max income:', max_income)
        pew_bins.append(max_income)

        print('=== CALCULATING USER BINS ===')
        cumulative_percents = np.cumsum(pew_percs) * 100
        cumulative_percents[-1] = 100
        percentiles = np.percentile(income, cumulative_percents)
        bins = [0] + list(percentiles)
        print('bins:', bins)

        print('=== VERIFY REDISTRIBUTION WORKED ===')
        hist, _ = np.histogram(income, bins=bins)
        bin_percs = hist / total
        print('pew_percs:', pew_percs)
        print('redist_percs:', bin_percs)

        print('=== UPDATING REDISTRIBUTED INCOME ===')
        for pew_min_bin, pew_max_bin, min_bin, max_bin in zip(pew_bins, pew_bins[1:], bins, bins[1:]):
            print('pew bins:', pew_min_bin, pew_max_bin)
            query = 'update {table} set income_redist = (({max_t2} - {min_t}) * (income - {min_s}) / ({max_s2} - {min_s}) + {min_t}) where income >= {min_s} and income < {max_s}'.format(
                table=table,
                min_t=pew_min_bin,
                max_t=pew_max_bin,
                max_t2=min(pew_max_bin, 1e9),
                min_s=min_bin,
                max_s=max_bin,
                max_s2=min(max_bin, 1e9)
            )
            print("->",query+";")
            conn.execute(query)

    print('=== REDISTRIBUTION COMPLETE ===')


if __name__ == "__main__":
    args = get_args()
    main(args)
