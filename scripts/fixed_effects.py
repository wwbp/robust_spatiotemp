"""
run as `python3 fixed_effects.py`
"""

import matplotlib
matplotlib.use('Agg')

import os, time, json, datetime, sys

from pymysql import cursors, connect
from tqdm import tqdm

import seaborn as sns

import pandas as pd
import statsmodels.formula.api as smf
import statsmodels as sm

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Open default connection
print('Connecting to MySQL...')
connection  = connect(read_default_file="~/.my.cnf")

# Get supplemental data
county_info = pd.read_csv("/data/smangalik/county_fips_data.csv",encoding = "utf-8")
county_info['cnty'] = county_info['fips'].astype(str).str.zfill(5)

counties_we_care_about = ['01073','48113','36103']

def yearweek_to_dates(yw):
  year, week = yw.split("_")
  year, week = int(year), int(week)

  first = datetime.datetime(year, 1, 1)
  base = 1 if first.isocalendar()[1] == 1 else 8
  monday = first + datetime.timedelta(days=base - first.isocalendar()[2] + 7 * (week - 1))
  sunday = monday + datetime.timedelta(days=6)
  thursday = monday + datetime.timedelta(days=3)
  return monday, thursday, sunday

def county_list_to_full_df(county_list):
  rows = []
  for cnty in county_list:
      for yw in list(county_feats[cnty].keys()): # for each valid yw
        year, week = yw.split("_")
        monday, thursday, sunday = yearweek_to_dates(yw)
        yearweek_cnty = "{}:{}".format(yw,cnty)
        year_cnty = "{}:{}".format(year,cnty)
        avg_anx = county_feats[cnty][yw]['ANX_SCORE']
        avg_dep = county_feats[cnty][yw]['DEP_SCORE']

        row = {
          "date":monday,
          'yearweek_cnty':yearweek_cnty,
          'year_cnty':year_cnty,
          'yearweek':yw,
          'year':year,
          'cnty':cnty,
          'avg_anx':avg_anx,
          'avg_dep':avg_dep
        }
        rows.append(row)

  df = pd.DataFrame.from_dict(rows)
  df = pd.merge(df,county_info[['cnty','state_name','region_name']],on='cnty')
  df['yearweek_state'] = df['yearweek'] + ":" + df['state_name']

  # GROUP BY if necessary
  #df.set_index('date', inplace=True)
  #df = df.groupby(pd.Grouper(freq='Q')).mean() # Q = Quarterly, M = Monthly

  return df

with connection:
  with connection.cursor(cursors.SSCursor) as cursor:
    print('Connected to',connection.host)

    # Get county feat information
    #county_feats_json = "/data/smangalik/county_feats_ctlb_30user.json" # /data/smangalik/county_feats_ctlb_X0user.json
    
    #county_feats_json = "/data/smangalik/county_feats_ctlb_nostd.json" # non-standardized experiment. 500GFT

    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt50user_ywcnty.json"       # 50 GFT old scaled
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user_ywcnty.json"      # 200 GFT old scaled
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt500user_ywcnty.json"      # 500 GFT old scaled
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt300user_ywsupercnty.json" # 300 GFT old scaled

    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user_ywcnty_notscaled.json" # 200 GFT, no-scale, 3 sigma
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user05scale6sig_ywcnty.json" # 200 GFT, old scaling

    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt50user05scale_ywcnty.json"  #  50GFT fix scaling
    county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user05scale_ywcnty.json" # 200GFT fix scaling
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt500user05scale_ywcnty.json" # 500GFT fix scaling
    
    
    print("Running on",county_feats_json)
    if not os.path.isfile(county_feats_json):
      print("County data not available")
      sys.exit(1)
    print("\nImporting produced county features")
    with open(county_feats_json) as json_file:
        county_feats = json.load(json_file)
    all_counties = county_feats.keys()
    county_list = all_counties

    # Process Language Based Assessments data
    lba_full = county_list_to_full_df(county_list)
    print("LBA (full)\n",lba_full.head(10))
    print('N =',len(lba_full),'covering',lba_full['cnty'].nunique(),'counties and',lba_full['yearweek'].nunique(),'weeks\n');
    #print("min =",lba_full['avg_dep'].min(),"; max =",lba_full['avg_dep'].max(), "std =",lba_full['avg_dep'].std())

    # Process Gallup COVID Panel
    gallup_gft = 0
    sql = "select fips as cnty, yearweek, WEB_worryF, WEC_sadF, neg_affect_lowArousal, neg_affect_highArousal, neg_affect, pos_affect, affect_balance from gallup_covid_panel_micro_poll.old_hasSadBefAug17_recodedEmoRaceGenPartyAge_v3_02_15;"
    gallup = pd.read_sql(sql, connection)
    print('Gallup by yw_user: N =',\
      len(gallup),'covering',gallup['cnty'].nunique(),'counties and',gallup['yearweek'].nunique(),'weeks');
    gallup_county_counts = gallup['cnty'].value_counts()
    gallup_passing_counties = list(gallup_county_counts[gallup_county_counts >= gallup_gft].index)
    gallup = gallup[gallup['cnty'].isin(gallup_passing_counties)]
    gallup = gallup.groupby(by=["yearweek","cnty"]).mean().reset_index() # aggregate to yearweek_cnty so we share group_id with LBA
    gallup['yearweek'] = gallup['yearweek'].astype(str)
    gallup['yearweek'] = gallup['yearweek'].str[:4] + "_" + gallup['yearweek'].str[4:]
    gallup['yearweek_cnty'] = gallup['yearweek'] + ":" + gallup['cnty']
    gallup = pd.merge(gallup,county_info[['cnty','state_name','region_name']],on='cnty')
    gallup['yearweek_state'] = gallup['yearweek'] + ":" + gallup['state_name']
    print("\nGallup COVID Panel (AVG by yearweek_cnty)\n",gallup.head(10))
    print("Gallup by yw_cnty w/ {}GFT: N =".format(gallup_gft),\
      len(gallup),'covering',gallup['cnty'].nunique(),'counties and',gallup['yearweek'].nunique(),'weeks');

    # Prepare Fixed Effects Data
    gallup_cols = ['WEC_sadF', 'WEB_worryF'] + ['yearweek_cnty']
    data = gallup[gallup_cols].merge(lba_full, on='yearweek_cnty')

    # Allow space time aggregation
    
    print("\nMerged LBA and Gallup\n",data.head())
    print('N =',len(data),'covering',data['cnty'].nunique(),'counties and',data['yearweek'].nunique(),'weeks');

    print("\nData Stats")
    print("data['WEB_worryF'].std() =",data['WEB_worryF'].std())
    print("data['WEC_sadF'].std()   =",data['WEC_sadF'].std())
    print("data['avg_anx'].std()    =",data['avg_anx'].std())
    print("data['avg_dep'].std()    =",data['avg_dep'].std())
    print()

    # County Center All The Outcomes
    entity = "cnty"
    outcomes = ['avg_dep','avg_anx','WEC_sadF','WEB_worryF']
    avg_outcomes = ["avg({})".format(x) for x in outcomes]
    mean_data = data.groupby(entity)[outcomes].mean().reset_index() # find average within each entity
    
    print("mean_data\n",mean_data.head())
    mean_data.columns = [entity]+avg_outcomes
    county_centered_data = data.merge(mean_data, on=entity) # merge on the average values
    county_centered_data[outcomes] = county_centered_data[outcomes] - county_centered_data[avg_outcomes].values # subtract off the average values
    county_centered_data = county_centered_data.drop(columns=avg_outcomes)

    # TODO confirm if standardizing the county centered data is good
    cols_to_std = []
    #cols_to_std = ['WEB_worryF','WEC_sadF','avg_anx','avg_dep']
    for col in cols_to_std:
      county_centered_data[col] = county_centered_data[col] / county_centered_data[col].std()


    print("\nCounty Centered Data\n",county_centered_data.head())
    print('N =',len(county_centered_data),'covering',county_centered_data['cnty'].nunique(),'counties and',county_centered_data['yearweek'].nunique(),'weeks\n');

    print("\nCounty Centered Data Stats")
    print("county_centered_data['WEB_worryF'].std() =",county_centered_data['WEB_worryF'].std())
    print("county_centered_data['WEC_sadF'].std()   =",county_centered_data['WEC_sadF'].std())
    print("county_centered_data['avg_anx'].std()    =",county_centered_data['avg_anx'].std())
    print("county_centered_data['avg_dep'].std()    =",county_centered_data['avg_dep'].std())
    print()

    # Run OLS Model
    formulas = []
    formulas.append("WEC_sadF ~ avg_dep + region_name  -1") 
    formulas.append("WEB_worryF ~ avg_anx + region_name -1") 
    formulas.append("WEC_sadF ~ avg_anx + region_name  -1" )
    formulas.append("WEB_worryF ~ avg_dep + region_name  -1") 
    
    for formula in formulas:
      print('\t\tCounty Centered OLS on',formula)
      mod = smf.ols(formula, data=county_centered_data).fit()
      #print(mod.summary().tables[0])
      print(mod.summary().tables[1])


    # Run Mixed Linear Model
    formula = formulas[0]
    md = smf.mixedlm(formula=formula, data=data, groups=data["region_name"])
    mdf = md.fit()
    print('\n',mdf.summary())


    # Run correlations (can be done on any level)
    group_on = 'cnty' # yearweek = week x national, yearweek_cnty = week x county, yearweek_state = week x state, cnty = year x county
    merge = data.groupby(group_on).mean().reset_index()
    print("\nAggregated Data for Correlation\n",merge.head(5),'\n...')
    corr = merge.corr(method="pearson")
    print("\nLBA vs Gallup COVID by",group_on,'\n', corr)
    print(len(merge),"samples used for correlation")
    corr_plot = sns.heatmap(corr.head(2), center=0, square=True, linewidths=.5, annot=True)
    corr_plot.figure.savefig("LBA vs Gallup County Centered Data.png", bbox_inches='tight')


    # Make Gallup into feat table: ['group_id', 'feat', 'value', 'group_norm']
    # gallup_worry = gallup.copy(deep=True)
    # gallup_worry['feat'] = "WEB_worryF"
    # gallup_worry['value'] = gallup_worry['WEB_worryF']
    # gallup_sad = gallup.copy(deep=True)
    # gallup_sad['feat'] = "WEC_sadF"
    # gallup_sad['value'] = gallup_worry['WEC_sadF']
    # gallup_mysql = pd.concat([gallup_worry, gallup_sad])
    # gallup_mysql = gallup_mysql.rename(columns={"yearweek_cnty":"group_id"})
    # gallup_mysql["group_norm"] = gallup_mysql["value"]
    # gallup_mysql = gallup_mysql[['group_id', 'feat', 'value', 'group_norm']]
    # print(gallup_mysql)

    # from sqlalchemy import create_engine
    # from sqlalchemy.engine.url import URL
    # myDB = URL(drivername='mysql', host='localhost',
    #   database='ctlb2', query={ 'read_default_file' : "~/.my.cnf" }
    # )
    # engine = create_engine(name_or_url=myDB)

    # gallup_mysql.to_sql("feat$gallup_covid$yw_cnty",con=engine, if_exists='replace', index=False)