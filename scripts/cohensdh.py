# Intended to run on feature tables with `group_id` of format "yearweek:county"

from pymysql import cursors, connect
import pandas as pd
import numpy as np
import random, sys
import scipy.stats as ss
from utils import yearweek_to_dates, date_to_quarter

sig_threshold = 0.1 # 0.3 for gallup; 0.1 for LBA 

def permutation_cohens_d(x, n_perms=30, ut=3):
    if len(x) < ut: return np.nan, np.nan
    if np.std(x) == 0: return np.nan, np.nan
    ds = []
    std = np.std(x)
    random.seed(a=25, version=2)
    for _ in range(n_perms):
        shuffled = random.sample(x, len(x))
        split_index = len(shuffled)//2
        a,b = shuffled[:split_index], shuffled[split_index:]
        d = abs(np.mean(a) - np.mean(b)) / std
        ds.append(d)
    avg_d = np.mean(ds)
    d_stderr = np.std(ds) / np.sqrt(len(ds))
    return avg_d, d_stderr

def cohens_d(x, ut=3):
    if len(x) < ut: return np.nan
    if np.std(x) == 0: return np.nan
    random.seed(a=25, version=2)
    shuffled = random.sample(x, len(x))
    split_index = len(shuffled)//2
    a,b = shuffled[:split_index], shuffled[split_index:]
    d = abs(np.mean(a) - np.mean(b)) / np.std(x)
    return d

def permutation_cohens_h(x, n_perms=30, ut=3):
    if len(x) < ut: return np.nan, np.nan
    if np.std(x) == 0: return np.nan, np.nan
    hs = []
    random.seed(a=25, version=2)
    for _ in range(n_perms):
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

def cohens_h(x, ut=3):
    if len(x) < ut: return np.nan
    random.seed(a=25, version=2)
    shuffled = random.sample(x, len(x))
    split_index = len(shuffled)//2
    a,b = shuffled[:split_index], shuffled[split_index:]
    p1 = sum(a)/len(a)
    p2 = sum(b)/len(b)
    h = abs(2 * (np.arcsin(np.sqrt(p1))- np.arcsin(np.sqrt(p2))))
    return h

def get_iccs(df, var="wavg_score", entity="cnty", ut=0):

    # data = [user_id, entity, scoreCol] e.g. [user, yearweek_cnty, wavg_score]
    #                                    e.g. [user, yearweek_cnty, WEC_sadF]

    data = df[[var,entity]].copy(deep=True)
    data = data.dropna()
    
    entity_counts = data[entity].value_counts().loc[lambda x: x>=ut]

    if len(entity_counts.values) <= 1:
        print("Not enough entities w/ UT = " + str(ut))
        return 0, 0, 0, 1

    avg_per_entity = sum(entity_counts.values)/len(entity_counts.values)

    entities_passing_check = entity_counts.index.tolist()
    data = data.loc[data[entity].isin(entities_passing_check)]
    
    overall_mean = data[var].mean()
    data['overall_mean'] = overall_mean
    ss_total = sum((data[var] - data['overall_mean'])**2)
    
    group_means = data.groupby(entity).mean()
    group_means = group_means.rename(columns = {var: 'group_mean'})
    data = data.merge(group_means, left_on=entity, right_index=True)
    
    ss_residual = sum((data[var] - data['group_mean'])**2)
    ss_explained = sum((data['overall_mean_y'] - data['group_mean'])**2)
    
    n_groups = len(set(data[entity]))
    n_obs = data.shape[0]
    
    df_residual = n_obs - n_groups
    ms_residual = ss_residual / df_residual
    
    df_explained = n_groups - 1
    ms_explained = ss_explained / df_explained
    
    f = ms_explained / ms_residual
    p_value = 1 - ss.f.cdf(f, df_explained, df_residual)

    ICC_1 = (ms_explained - ms_residual) / (ms_explained + (n_groups - 1)*ms_residual)

    ICC_2 = (avg_per_entity*ICC_1) / (1 + (avg_per_entity-1)*ICC_1)

    ICC_2s = [(v*ICC_1) / (1 + (v-1)*ICC_1) for v in entity_counts.values]
    ICC2_mean = np.mean(ICC_2s)
    ICC2_std = np.std(ICC_2s)
    

    print(ut)
    print(ICC2_mean)
    print(ICC2_std)
    print(len(entities_passing_check))
    print(avg_per_entity)
    if ":" in data[entity].iloc[0]:
        data["space_entity"] = data[entity].apply(lambda x: x.split(":")[1])
        print(len(set(data["space_entity"])))
    
    return ICC_1, ICC2_mean, ICC2_std, p_value
   


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

    # Latest ctlb2
    # tables = ["ctlb2.feat$dd_daa_c2adpt_ans_nos$timelines19to20_lex_3upts$yw_cnty"]
    # feat_val_col = "wavg_score"
    # groupby_col = "cnty" # cnty, yearweek
    # feat_value = "ANX_SCORE" # ANX_SCORE, DEP_SCORE
    # filter = "WHERE feat = '{}'".format(feat_value)
    # relevant_columns = "*"
    # database = 'ctlb2'

    # Gallup COVID Panel
    tables = ["gallup_covid_panel_micro_poll.old_hasSadBefAug17_recodedEmoRaceGenPartyAge_v3_02_15"]
    feat_val_col = "WEC_sadF" # WEB_worryF, WEC_sadF, pos_affect, neg_affect
    groupby_col = "yearweek_cnty" # cnty, yearweek, state_name, yearweek_cnty, division_name, yearweek_msa, month_msa, month_state, quarter_state, quarter_division
    feat_value = feat_val_col
    filter = "WHERE {} IS NOT NULL".format(feat_val_col)
    relevant_columns = "fips, yearweek, WEA_enjoyF, WEB_worryF, WEC_sadF, WEI_depressionF, WEJ_anxietyF, pos_affect, neg_affect"
    database = 'gallup_covid_panel_micro_poll'

    # Census Household Pulse
    # tables = ["household_pulse.pulse"]
    # filter = ""
    # feat_val_col = "phq2_sum" # "phq2_sum"
    # groupby_col = "week_msa" # EST_MSA, state, WEEK, week_msa, state_week
    # feat_value = feat_val_col # phq2_sum
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
        # (Optional) 2020 minus 2019
        df_2019 = df[df['yearweek'].str.startswith("2019")]
        df_2020 = df[df['yearweek'].str.startswith("2020")]
        df_2019['week'] = df_2019['yearweek'].str.split(":",expand=True)[0].str.split("_",expand=True)[1]
        df_2020['week'] = df_2020['yearweek'].str.split(":",expand=True)[0].str.split("_",expand=True)[1]
        df = df_2020.merge(df_2019[['cnty','week','feat',feat_val_col]], on=['cnty','week','feat'], suffixes= ("_2020", "_2019"))
        df[feat_val_col] =  df[feat_val_col+'_2020'] - df[feat_val_col+'_2019'] + np.mean(df[feat_val_col+'_2019'])
        pass

    if database == 'household_pulse':
        df['state_week'] = df['state'] + "_" + df['WEEK'].map(str)

        if groupby_col == 'week_msa':
            df['week_msa'] = df['WEEK'].map(str) + ":" + df['EST_MSA']
            df = df.dropna()

    if database == 'gallup_covid_panel_micro_poll':
        df = df.rename(columns={"fips":"cnty"})
        df['yearweek'] = df['yearweek'].str[:4] + "_" + df['yearweek'].str[4:]
        df['yearweek_cnty'] = df['yearweek'] + ":" + df['cnty']
        df['date'] = df['yearweek'].apply(lambda x: yearweek_to_dates(x)[1])
        df['month'] = df['date'].apply(lambda x: str(x.month))
        df['quarter'] = df['date'].apply(lambda x: date_to_quarter(x))
        df = pd.merge(df,county_info[['cnty','state_name','region_name','division_name']],on='cnty')
        df['week_region'] = df['yearweek'] + ":" + df['region_name']
        df['month_region'] = df['month'] + ":" + df['region_name']
        df['month_state'] = df['month'] + ":" + df['state_name']
        df['month_cnty'] = df['month'] + ":" + df['cnty']
        df['quarter_cnty'] = df['quarter'] + ":" + df['cnty']
        df['quarter_state'] = df['quarter'] + ":" + df['state_name']
        df['quarter_division'] = df['quarter'] + ":" + df['division_name']
        df['quarter_region'] = df['quarter'] + ":" + df['region_name']
        if 'msa' in groupby_col:
            df = pd.merge(df,msa_info[['cnty','msa']],on='cnty')
            df = df[df['msa'].notna()]
            df['yearweek_msa'] = df['yearweek'] + ":" + df['msa']
            df['month_msa'] = df['month'] + ":" + df['msa']

    # Peek the cleaned data
    print("Cleaned data")
    print(df)

    # Calculate ICCs BEFORE group by
    for ut in [0,10,20,30,50,100,200,300,500,1000]:
        print("\n\n\nCalculating ICCs for",feat_value,"by",groupby_col,'at UT =',ut)
        ICC_1, ICC2_mean, ICC2_std, p_value = get_iccs(df, var=feat_val_col, entity=groupby_col, ut=ut)
        print('\nicc1 =', ICC_1)
        print('icc2 =', ICC2_mean, "(",ICC2_std,")")
        print('p_value =', p_value)

    grouped = df.groupby(groupby_col)[feat_val_col].apply(list).reset_index(name=feat_value)
    grouped[feat_value] = grouped[feat_value].apply(lambda x: [i for i in x if str(i) != "nan"]) # clean NaNs
    
    grouped['std'] = [np.array(x).std() for x in grouped[feat_value].values]
    grouped['n'] = [len(x) for x in grouped[feat_value].values]

    min_entries = 10 # can be interpreted as UT, normally 10
    grouped = grouped[grouped['n'] >= min_entries] # list has more than min_entries values

    # Calculate Cohen's D
    grouped['d_sample'] = [cohens_d(x, ut=min_entries) for x in grouped[feat_value].values]
    grouped['h_sample'] = [cohens_h(x, ut=min_entries) for x in grouped[feat_value].values]

    # Calculate Permutation Test of D
    grouped[["d_perm","d_perm_stderr"]] = grouped.apply(lambda x: permutation_cohens_d(x[feat_value], ut=min_entries), axis=1,result_type ='expand')
    grouped[["h_perm","h_perm_stderr"]] = grouped.apply(lambda x: permutation_cohens_h(x[feat_value], ut=min_entries), axis=1,result_type ='expand')
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

    print("Min Entires to Consider = {}".format(min_entries))
    print("Average D  = {}; Stderr D = {}".format( round(np.mean(grouped['d_sample']),4), round(np.std(grouped['d_sample'])/np.sqrt(len(grouped)),4) ))
    print("Average Perm D  = {}; Average Perm D + CI = {}; Stderr Perm D {}".format( round(np.mean(grouped['d_perm']),4), round(np.mean(grouped['d_perm+ci']),4), round(np.std(grouped['d_perm'])/np.sqrt(len(grouped)),4) ))
    print("Average Perm H  = {}; Average Perm H + CI = {}; Stderr Perm H {}".format( round(np.mean(grouped['h_perm']),4), round(np.mean(grouped['h_perm+ci']),4), round(np.std(grouped['h_perm'])/np.sqrt(len(grouped)),4) ))
    print("Overall Median / Mean n = {} / {}".format( np.median(grouped['n']), round(np.mean(grouped['n'])), 2) )
    
    # Do Cohen's D of groupby entity scores
    for ut in [10,20,30,50,100,200,300,400,500,1000]:
        data = df.copy(deep=True)
        entity_counts = data[groupby_col].value_counts().loc[lambda x: x>=ut]
        if len(entity_counts.values) <= 1:
            print("\nNot enough entities w/ UT = " + str(ut))
            continue
        data = data.loc[data[groupby_col].isin( entity_counts.index.tolist() )]
        groupby_scores = data.groupby(groupby_col).mean().reset_index()[feat_val_col]
        avg_d, d_stderr = permutation_cohens_d(groupby_scores.to_list(), n_perms=30)
        print("\nAt UT = {} with n = {}".format(ut,len(entity_counts)))
        print("Perm D of {} scores = {}; Stderr Perm D {}".format( groupby_col, round(avg_d,4), round(d_stderr,4) ))

    #print("all Ds\n",grouped['d_perm'].dropna())
    #print("Std Dev Perm D = {}; Std Dev Perm D + CI = {}".format( np.std(grouped['d_perm']), np.std(grouped['d_perm+ci']) ))
    print()

    ignore_cols = ['d_perm_stderr','h_perm_stderr']
    print("\nHere are the significant findings:\n", significant.drop(ignore_cols, axis=1))

    print("\nHere are the insignificant findings:\n", insignificant.drop(ignore_cols, axis=1))

    reliable_entities = list(significant[groupby_col])
    print("\nReliable entities:",reliable_entities)
    #print("Smallest n was", min(significant['n']))

    # Gives the option of outputting a "good" gallup dataset
    # if database == "gallup_covid_panel_micro_poll" and groupby_col == "cnty":
    #     df_cnty = df.groupby(groupby_col).mean().reset_index()
    #     df_cnty = pd.merge(df_cnty,county_info[['cnty','state_name','region_name','division_name']],on='cnty')
    #     df_cnty = df_cnty[df_cnty[groupby_col].isin(reliable_entities)]
    #     df_cnty.to_csv("~/gallup_covid_{}_reliable.csv".format(groupby_col),index=False)
    #     print("Agg on", groupby_col ,"\n",df_cnty)


if __name__ == "__main__":
    main()
