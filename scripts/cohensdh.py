# Intended to run on feature tables with `group_id` of format "yearweek:county"

from pymysql import cursors, connect
import pandas as pd
import numpy as np
import random, sys
import scipy.stats as ss
from utils import yearweek_to_dates, date_to_quarter

sig_threshold = 0.1

def permutation_cohens_d(x, n_perms=30, min_entries=2):
    if len(x) <= min_entries: return np.nan, np.nan
    if np.std(x) < 1e-6: return np.nan, np.nan
    ds = []
    std = np.std(x)
    random.seed(a=25, version=2)
    for i in range(n_perms):
        shuffled = random.sample(x, len(x))
        split_index = len(shuffled)//2
        a,b = shuffled[:split_index], shuffled[split_index:]
        d = abs(np.mean(a) - np.mean(b)) / std
        ds.append(d)
    avg_d = np.mean(ds)
    d_stderr = np.std(ds) / np.sqrt(len(ds))
    return avg_d, d_stderr

def cohens_d(x, min_entries=2):
    if len(x) <= min_entries: return np.nan
    if np.std(x) < 1e-6: return np.nan
    random.seed(a=25, version=2)
    shuffled = random.sample(x, len(x))
    split_index = len(shuffled)//2
    a,b = shuffled[:split_index], shuffled[split_index:]
    d = abs(np.mean(a) - np.mean(b)) / np.std(x)
    return d

def permutation_cohens_h(x, n_perms=30, min_entries=2):
    if len(x) <= min_entries: return np.nan, np.nan
    if np.std(x) < 1e-6: return np.nan, np.nan
    hs = []
    random.seed(a=25, version=2)
    for i in range(n_perms):
        shuffled = random.sample(x, len(x))
        split_index = len(shuffled)//2
        a,b = shuffled[:split_index], shuffled[split_index:]
        p1 = sum(a)/len(a)
        p2 = sum(b)/len(b)
        h = abs(2 * (np.arcsin(np.sqrt(p1))- np.arcsin(np.sqrt(p2))))
        hs.append(h)
    avg_h = np.mean(hs)
    h_stderr = np.std(hs) / np.sqrt(len(hs))
    return avg_h, h_stderr

def cohens_h(x, min_entries=2):
    if len(x) <= min_entries: return np.nan
    random.seed(a=25, version=2)
    shuffled = random.sample(x, len(x))
    split_index = len(shuffled)//2
    a,b = shuffled[:split_index], shuffled[split_index:]
    p1 = sum(a)/len(a)
    p2 = sum(b)/len(b)
    h = abs(2 * (np.arcsin(np.sqrt(p1))- np.arcsin(np.sqrt(p2))))
    return h


def main():

    # experimenting with n
    # for n in range(3,100,5):
    #     x = ss.bernoulli.rvs(size=n, p=0.5)
    #     d, _ = permutation_cohens_d(list(x))
    #     print(x,n,d,'\n')
    # sys.exit()

    # Open default connection
    print('Connecting to MySQL...')
    connection  = connect(read_default_file="~/.my.cnf")

    # Get supplemental data
    county_info = pd.read_csv("county_fips_data.csv",encoding = "utf-8")
    county_info['cnty'] = county_info['fips'].astype(str).str.zfill(5)
    msa_info = pd.read_csv("county_msa_mapping.csv")
    msa_info['cnty'] = msa_info['fips'].astype(str).str.zfill(5)

    # NOT INDIVIDUAL RESULTS
    # tables = ["ctlb2.feat$dd_depAnxLex$timelines2019$yw_cnty$1gra",
    #           "ctlb2.feat$dd_depAnxLex$timelines2020$yw_cnty$1gra"]
    # feat_val_col = "group_norm"
    # groupby_col = "yearweek" # cnty, yearweek
    # feat_value = "DEP_SCORE" # ANX_SCORE, DEP_SCORE
    # filter = "WHERE feat = '{}'".format(feat_value)
    # relevant_columns = "*"
    # database = 'ctlb2'

    # Gallup COVID Panel
    tables = ["gallup_covid_panel_micro_poll.old_hasSadBefAug17_recodedEmoRaceGenPartyAge_v3_02_15"]
    feat_val_col = "WEC_sadF" # WEB_worryF, WEC_sadF, pos_affect, neg_affect
    groupby_col = "cnty" # cnty, yearweek, state_name yearweek_cnty, division_name, yearweek_msa, month_msa, month_state, quarter_state, quarter_division
    feat_value = feat_val_col
    filter = "WHERE {} IS NOT NULL".format(feat_val_col)
    relevant_columns = "fips, yearweek, WEA_enjoyF, WEB_worryF, WEC_sadF, WEI_depressionF, WEJ_anxietyF, pos_affect, neg_affect"
    database = 'gallup_covid_panel_micro_poll'

    # Census Household Pulse
    # tables = ["household_pulse.pulse"]
    # filter = ""
    # feat_val_col = "gad2_sum"
    # groupby_col = "state_week" # EST_MSA, state, WEEK, msa_week, state_week
    # feat_value = "gad2_sum" # phq2_sum
    # relevant_columns = ",".join(['WEEK','state','EST_ST','EST_MSA','gad2_sum','phq2_sum'])
    # database = 'household_pulse'


    sql = "SELECT {} FROM {} {}".format(
        relevant_columns, tables[0], filter
    )
    df = pd.read_sql(sql, connection)

    for table in tables[1:]:
        sql = "SELECT {} FROM {} {}".format(
            relevant_columns, table, filter
        )
        df = pd.read_sql(sql, connection).append(df, ignore_index=True)


    print("Cleaning up columns")
    if database == 'ctlb2':
        df = df[~df['group_id'].str.startswith((':'))]
        df[['yearweek','cnty']] = df['group_id'].str.split(":",expand=True,)

    if database == 'household_pulse':
        df['state_week'] = df['state'] + "_" + df['WEEK'].map(str)
        df['msa_week'] = df['EST_MSA'] + "_" + df['WEEK'].map(str)

    if database == 'gallup_covid_panel_micro_poll':
        df = df.rename(columns={"fips":"cnty"})
        df['yearweek'] = df['yearweek'].str[:4] + "_" + df['yearweek'].str[4:]
        df['yearweek_cnty'] = df['yearweek'] + ":" + df['cnty']
        df['date'] = df['yearweek'].apply(lambda x: yearweek_to_dates(x)[1])
        df['month'] = df['date'].apply(lambda x: str(x.month))
        df['quarter'] = df['date'].apply(lambda x: date_to_quarter(x))
        df = pd.merge(df,county_info[['cnty','state_name','region_name','division_name']],on='cnty')
        df['month_state'] = df['month'] + ":" + df['state_name']
        df['quarter_state'] = df['quarter'] + ":" + df['state_name']
        df['quarter_division'] = df['quarter'] + ":" + df['division_name']
        if 'msa' in groupby_col:
            df = pd.merge(df,msa_info[['cnty','msa']],on='cnty')
            df = df[df['msa'].notna()]
            df['yearweek_msa'] = df['yearweek'] + ":" + df['msa']
            df['month_msa'] = df['month'] + ":" + df['msa']

    # Peek the cleaned data
    print(df)

    grouped = df.groupby(groupby_col)[feat_val_col].apply(list).reset_index(name=feat_value)
    grouped[feat_value] = grouped[feat_value].apply(lambda x: [i for i in x if str(i) != "nan"]) # clean NaNs
    grouped['std'] = [np.array(x).std() for x in grouped[feat_value].values]


    grouped['n'] = [len(x) for x in grouped[feat_value].values]
    # min_len = 10
    # grouped = grouped[grouped['n'] >= min_len] # list has more than 2 values

    # Calculate Cohen's D
    min_entries = 10
    grouped['d_sample'] = [cohens_d(x, min_entries=min_entries) for x in grouped[feat_value].values]
    grouped['h_sample'] = [cohens_h(x, min_entries=min_entries) for x in grouped[feat_value].values]

    # Calculate Permutation Test of D
    grouped[["d_perm","d_perm_stderr"]] = grouped.apply(lambda x: permutation_cohens_d(x[feat_value], min_entries=min_entries), axis=1,result_type ='expand')
    grouped[["h_perm","h_perm_stderr"]] = grouped.apply(lambda x: permutation_cohens_h(x[feat_value], min_entries=min_entries), axis=1,result_type ='expand')
    grouped['d_perm+ci'] = grouped['d_perm'] + (grouped['d_perm_stderr'] * 1.96)
    grouped['h_perm+ci'] = grouped['h_perm'] + (grouped['h_perm_stderr'] * 1.96)


    # Filter D Values
    if database == 'gallup_covid_panel_micro_poll':
        significant_mask = grouped['h_perm+ci'] < sig_threshold
    else:
        significant_mask = grouped['d_perm+ci'] < sig_threshold
    significant = grouped[significant_mask]
    insignificant = grouped[~significant_mask]

    # Print out results
    if database == 'gallup_covid_panel_micro_poll':
        print("\nFor {} and {} we find the following {}/{} groups to have a Permutation Cohen's H less than {}:\n".format(
        feat_value, groupby_col, len(significant), len(grouped), sig_threshold))
    else:
        print("\nFor {} and {} we find the following {}/{} groups to have a Permutation Cohen's D less than {}:\n".format(
        feat_value, groupby_col, len(significant), len(grouped), sig_threshold))


    print("Average Perm D  = {}; Average Perm D + CI = {}".format( round(np.mean(grouped['d_perm']),4), round(np.mean(grouped['d_perm+ci']),4) ))
    print("Average Perm H  = {}; Average Perm H + CI = {}".format( round(np.mean(grouped['h_perm']),4), round(np.mean(grouped['h_perm+ci']),4) ))
    print("Overall Median / Mean n = {} / {}".format( np.median(grouped['n']), round(np.mean(grouped['n'])), 2) )
    #print("Std Dev Perm D = {}; Std Dev Perm D + CI = {}".format( np.std(grouped['d_perm']), np.std(grouped['d_perm+ci']) ))
    print()

    ignore_cols = ['d_perm_stderr','h_perm_stderr']
    print("\nHere are the significant findings:\n", significant.drop(ignore_cols, axis=1))

    print("\nHere are the insignificant findings:\n", insignificant.drop(ignore_cols, axis=1))

    reliable_entities = list(significant[groupby_col])
    print("\nReliable entities:",reliable_entities)

    if database == "gallup_covid_panel_micro_poll":
        df_cnty = df.groupby('cnty').mean().reset_index()
        df_cnty = pd.merge(df_cnty,county_info[['cnty','state_name','region_name','division_name']],on='cnty')
        df_cnty = df_cnty[df_cnty[groupby_col].isin(reliable_entities)]
        df_cnty.to_csv("~/gallup_covid_cnty.csv",index=False)
        print("Agg on County\n",df_cnty)


if __name__ == "__main__":
    main()
