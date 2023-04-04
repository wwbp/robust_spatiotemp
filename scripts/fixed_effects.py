"""
run as `python3 fixed_effects.py`
"""

import matplotlib
matplotlib.use('Agg')

import os, time, json, datetime, sys
import sqlalchemy

from pymysql import cursors, connect
from tqdm import tqdm
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

import seaborn as sns
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels as sm

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

GFT = 200 # now called CUT

# Open default connection
print('Connecting to MySQL...')
connection  = connect(read_default_file="~/.my.cnf")
myDB = URL(drivername='mysql', host='localhost',
  database='ctlb2', query={ 'read_default_file' : "~/.my.cnf" }
)
engine = create_engine(name_or_url=myDB)

# Get supplemental data
county_info = pd.read_csv("/data/smangalik/county_fips_data.csv",encoding = "utf-8")
county_info['cnty'] = county_info['fips'].astype(str).str.zfill(5)

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
        avg_anx = county_feats[cnty][yw]['ANX_SCORE']['score']
        avg_dep = county_feats[cnty][yw]['DEP_SCORE']['score']
        std_anx = county_feats[cnty][yw]['ANX_SCORE']['std_score']
        std_dep = county_feats[cnty][yw]['DEP_SCORE']['std_score']

        row = {
          "date":thursday,
          'yearweek_cnty':yearweek_cnty,
          'year_cnty':year_cnty,
          'yearweek':yw,
          'year':year,
          'cnty':cnty,
          'avg_anx':avg_anx,
          'avg_dep':avg_dep,
          'std_anx':std_anx,
          'std_dep':std_dep
        }
        rows.append(row)

  df = pd.DataFrame.from_dict(rows)
  df = pd.merge(df,county_info[['cnty','state_name','region_name']],on='cnty')
  df['yearweek_state'] = df['yearweek'] + ":" + df['state_name']

  # GROUP BY if necessary
  #df.set_index('date', inplace=True)
  #df = df.groupby(pd.Grouper(freq='Q')).mean() # Q = Quarterly, M = Monthly

  return df  

def df_printout(df, outcomes=None, space=None, time=None):
  print(df.head())
  print('N =',len(df),end="")
  if space:
    print(" over {} {}s".format(df[space].nunique(),space),end="")
  if time:
    print(" over {} {}s".format(df[time].nunique(),time),end="")
  print()
  if outcomes:
    for outcome in outcomes:
        print("-> avg({}) \t= {} ({})".format(outcome, df[outcome].mean(), df[outcome].std()))
  print()


with connection:
  with connection.cursor(cursors.SSCursor) as cursor:
    print('Connected to',connection.host)

    #county_feats_json = "/data/smangalik/feat_dd_depAnxLex_19to20_3upt{}user05fc_ywcnty.json".format(GFT) #16to16
    county_feats_json = "/data/smangalik/feat_dd_daa_c2adpt_ans_19to20_3upt{}user05fc_ywcnty.json".format(GFT) #16to8
    
    
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

    #  [Optional] 2020 minus 2019
    lba_full_2019 = lba_full[lba_full['yearweek'].str.startswith("2019")] # Separate Years
    lba_full_2020 = lba_full[lba_full['yearweek'].str.startswith("2020")]
    lba_full_2019['week'] = lba_full_2019['yearweek'].str.split(":",expand=True)[0].str.split("_",expand=True)[1] # Add week column
    lba_full_2020['week'] = lba_full_2020['yearweek'].str.split(":",expand=True)[0].str.split("_",expand=True)[1]
    lba_full_diff = lba_full_2020.merge(lba_full_2019[['cnty','week','avg_anx','avg_dep']], on=['cnty','week'], suffixes= ("_2020", "_2019")) # Merge 2019 yearweek scores onto 2020
    lba_full_diff['avg_anx'] =  lba_full_diff['avg_anx_2020'] - lba_full_diff['avg_anx_2019'] + np.mean(lba_full_diff['avg_anx_2019']) # scoreCol = scoreCol_2020 - scoreCol_2019 + mean(scoreCol_2019)
    lba_full_diff['avg_dep'] =  lba_full_diff['avg_dep_2020'] - lba_full_diff['avg_dep_2019'] + np.mean(lba_full_diff['avg_dep_2019'])
    lba_full = lba_full_diff 

    print("LBA (full)\n",lba_full.head(10))
    print('N =',len(lba_full),'covering',lba_full['cnty'].nunique(),'counties and',lba_full['yearweek'].nunique(),'weeks\n');
    #print("min =",lba_full['avg_dep'].min(),"; max =",lba_full['avg_dep'].max(), "std =",lba_full['avg_dep'].std())

    # Process Gallup COVID Panel
    gft = 0
    reliable_counties_07 = ['01003', '01073', '01089', '01097', '01117', '02020', '02170', '04003', '04013', '04015', '04019', '04021', '04025', '05007', '05119', '05143', '06001', '06007', '06009', '06013', '06017', '06019', '06023', '06029', '06037', '06041', '06047', '06053', '06055', '06057', '06059', '06061', '06065', '06067', '06071', '06073', '06075', '06077', '06079', '06081', '06083', '06085', '06087', '06089', '06095', '06097', '06099', '06107', '06111', '06113', '08001', '08005', '08013', '08031', '08035', '08041', '08059', '08067', '08069', '08123', '09001', '09003', '09005', '09007', '09009', '09011', '09013', '10003', '10005', '11001', '12001', '12005', '12009', '12011', '12015', '12017', '12019', '12021', '12031', '12033', '12053', '12057', '12069', '12071', '12073', '12081', '12083', '12086', '12089', '12091', '12095', '12097', '12099', '12101', '12103', '12105', '12109', '12113', '12115', '12117', '12119', '12127', '13051', '13057', '13059', '13067', '13077', '13089', '13113', '13121', '13135', '13139', '15001', '15003', '16001', '16005', '16027', '16055', '17019', '17031', '17043', '17089', '17097', '17111', '17113', '17119', '17143', '17167', '17179', '17197', '17201', '18003', '18039', '18057', '18063', '18081', '18089', '18097', '18109', '18127', '18141', '18157', '18163', '19013', '19103', '19113', '19153', '19163', '19169', '20045', '20091', '20103', '20173', '20177', '21067', '21111', '22033', '22051', '22071', '22103', '23005', '23011', '23019', '23031', '24003', '24005', '24017', '24021', '24025', '24027', '24031', '24033', '24510', '25001', '25003', '25005', '25009', '25011', '25013', '25015', '25017', '25021', '25023', '25025', '25027', '26021', '26049', '26065', '26077', '26081', '26093', '26099', '26111', '26125', '26139', '26145', '26161', '26163', '27003', '27019', '27035', '27037', '27053', '27109', '27123', '27137', '27141', '27145', '27163', '27171', '29019', '29047', '29077', '29095', '29097', '29099', '29165', '29183', '29189', '29510', '30031', '30111', '31055', '31109', '31153', '32003', '32031', '33011', '33013', '33015', '34001', '34003', '34005', '34007', '34013', '34015', '34017', '34019', '34021', '34023', '34025', '34027', '34029', '34031', '34035', '34037', '34039', '35001', '35013', '35049', '36001', '36005', '36007', '36027', '36029', '36047', '36051', '36055', '36059', '36061', '36063', '36065', '36067', '36071', '36081', '36083', '36085', '36087', '36091', '36093', '36101', '36103', '36111', '36119', '37019', '37021', '37025', '37051', '37063', '37067', '37071', '37081', '37119', '37129', '37133', '37135', '37147', '37183', '38017', '39017', '39025', '39035', '39041', '39049', '39057', '39061', '39085', '39089', '39093', '39095', '39099', '39103', '39113', '39151', '39153', '39155', '39165', '40027', '40031', '40109', '40143', '41003', '41005', '41017', '41019', '41029', '41039', '41041', '41043', '41047', '41051', '41067', '42003', '42007', '42011', '42013', '42017', '42019', '42027', '42029', '42041', '42043', '42045', '42049', '42069', '42071', '42075', '42077', '42079', '42081', '42091', '42095', '42101', '42125', '42129', '42133', '44003', '44007', '45013', '45019', '45045', '45051', '45063', '45079', '45083', '45091', '46103', '47037', '47065', '47093', '47125', '47149', '47157', '47165', '47187', '48027', '48029', '48039', '48041', '48085', '48091', '48113', '48121', '48141', '48157', '48167', '48181', '48187', '48201', '48209', '48215', '48251', '48303', '48309', '48329', '48339', '48355', '48423', '48439', '48451', '48453', '48485', '48491', '49005', '49011', '49035', '49045', '49049', '49053', '49057', '50007', '50023', '51003', '51013', '51041', '51059', '51061', '51087', '51095', '51107', '51153', '51161', '51177', '51179', '51510', '51550', '51760', '51810', '53005', '53009', '53011', '53033', '53035', '53053', '53057', '53061', '53063', '53067', '53073', '55009', '55025', '55039', '55079', '55087', '55101', '55105', '55131', '55133', '55139', '56021']
    gallup_gft = gft * 22 # GFT * 22 weeks
    sql = "select fips as cnty, yearweek, WEB_worryF, WEC_sadF, neg_affect_lowArousal, neg_affect_highArousal, neg_affect, pos_affect, affect_balance from gallup_covid_panel_micro_poll.old_hasSadBefAug17_recodedEmoRaceGenPartyAge_v3_02_15;"
    gallup = pd.read_sql(sql, connection)
    print('Gallup by yw_user: N =',\
      len(gallup),'covering',gallup['cnty'].nunique(),'counties and',gallup['yearweek'].nunique(),'weeks');
    print('Reliable Gallup by yw_user: N =',\
      len(gallup[gallup['cnty'].isin(reliable_counties_07)]),'covering',gallup[gallup['cnty'].isin(reliable_counties_07)]['cnty'].nunique(),'counties and',gallup[gallup['cnty'].isin(reliable_counties_07)]['yearweek'].nunique(),'weeks');
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

    # Filter to only Gallup reliable counties for d=0.7
    gallup = gallup[gallup['cnty'].isin(reliable_counties_07)].reset_index()
    #reliable_weekcounties_07 = ['2020_12:04013', '2020_12:06037', '2020_12:06065', '2020_12:06073', '2020_12:11001', '2020_12:17031', '2020_12:27053', '2020_12:36061', '2020_12:47037', '2020_12:48201', '2020_12:48453', '2020_12:49035', '2020_12:51059', '2020_12:53033', '2020_13:04013', '2020_13:04019', '2020_13:06001', '2020_13:06037', '2020_13:06059', '2020_13:06065', '2020_13:06073', '2020_13:06085', '2020_13:11001', '2020_13:12011', '2020_13:17031', '2020_13:24031', '2020_13:27053', '2020_13:36061', '2020_13:48029', '2020_13:48113', '2020_13:48201', '2020_13:53033', '2020_14:04013', '2020_14:06037', '2020_14:06067', '2020_14:06073', '2020_14:11001', '2020_14:17031', '2020_14:25017', '2020_14:26125', '2020_14:31055', '2020_14:36061', '2020_14:41051', '2020_14:48113', '2020_14:48201', '2020_14:51059', '2020_14:53033', '2020_15:04013', '2020_15:06037', '2020_15:06059', '2020_15:06065', '2020_15:06073', '2020_15:11001', '2020_15:17031', '2020_15:27053', '2020_15:48113', '2020_15:48201', '2020_15:48453', '2020_15:53033', '2020_16:04013', '2020_16:06013', '2020_16:06037', '2020_16:06059', '2020_16:06071', '2020_16:06073', '2020_16:06085', '2020_16:11001', '2020_16:17031', '2020_16:20091', '2020_16:25017', '2020_16:27053', '2020_16:29189', '2020_16:36029', '2020_16:36119', '2020_16:48029', '2020_16:48113', '2020_16:48201', '2020_16:53033', '2020_17:04013', '2020_17:06037', '2020_17:06065', '2020_17:06073', '2020_17:17031', '2020_17:36059', '2020_18:06037', '2020_18:17031', '2020_18:53033', '2020_19:06037', '2020_19:06073', '2020_19:11001', '2020_19:17031', '2020_20:04013', '2020_20:06037', '2020_20:11001', '2020_21:04013', '2020_21:06037', '2020_21:06073', '2020_21:17031', '2020_22:04013', '2020_22:06037', '2020_22:53033', '2020_23:04013', '2020_23:06037', '2020_23:17031', '2020_24:04013', '2020_24:06037', '2020_24:06059', '2020_24:17031', '2020_24:48201', '2020_25:06037', '2020_26:04013', '2020_26:06065', '2020_26:17031', '2020_27:04013', '2020_27:17031', '2020_27:53033', '2020_28:04013', '2020_28:06037', '2020_28:11001', '2020_29:04013', '2020_29:06037', '2020_30:04013', '2020_30:06037', '2020_30:17031', '2020_30:25017', '2020_30:27053', '2020_31:04013', '2020_32:04013', '2020_32:06037', '2020_32:17031', '2020_32:53033']
    #data = data[data['yearweek_cnty'].isin(reliable_weekcounties_07)]
    print("\nEnforce Gallup Reliable Counties at 1-h=0.7\n",gallup.head())
    print('N =',len(gallup),'covering',gallup['cnty'].nunique(),'counties and',gallup['yearweek'].nunique(),'weeks');

    # Melt into dlatk format
    gallup_melt = pd.melt(gallup.groupby(by="cnty").mean().reset_index(), id_vars=['cnty'], value_vars=['WEC_sadF','WEB_worryF']).reset_index()
    gallup_melt['cnty'] = gallup_melt['cnty'].astype(int)
    gallup_melt['group_norm'] = gallup_melt['value']
    gallup_melt = gallup_melt.rename(columns={"cnty": "group_id", "index":"id", "variable":"feat"})
    print("\nMelted Gallup",'\n',gallup_melt.sample(5))
    # Export Cross-Sectional data
    table_name = "feat$gallup_covid$cross20$cnty"
    gallup_melt.to_sql(table_name,con=engine,if_exists="replace",index=False,dtype={
      'id':sqlalchemy.types.INTEGER(),
      'group_id':sqlalchemy.types.INTEGER(),
      'feat':sqlalchemy.types.VARCHAR(length=25),
      'group_norm':sqlalchemy.types.FLOAT(precision=53)})
    with engine.connect() as con:
      con.execute('ALTER TABLE `{}` ADD PRIMARY KEY (`id`);'.format(table_name))
      con.execute('ALTER TABLE `{}` engine=myisam;'.format(table_name))
      con.execute('CREATE INDEX gdex ON `{}` (group_id);'.format(table_name))
      con.execute('CREATE INDEX fdex ON `{}` (feat);'.format(table_name))

    # Melt into dlatk format for MySQL
    lba_full_melt = lba_full[(lba_full['yearweek'] >= gallup['yearweek'].min()) & (lba_full['yearweek'] <= gallup['yearweek'].max())]
    lba_full_melt = pd.melt(lba_full_melt.groupby(by="cnty").mean().reset_index(), id_vars=['cnty'], value_vars=['avg_anx','avg_dep']).reset_index()
    lba_full_melt['group_norm'] = lba_full_melt['value']
    lba_full_melt['cnty'] = lba_full_melt['cnty'].astype(int)
    #lba_full_melt['group_norm'] = (lba_full_melt['value'] - lba_full_melt['value'].mean())/lba_full_melt['value'].std()
    lba_full_melt = lba_full_melt.rename(columns={"cnty": "group_id", "index":"id", "variable":"feat"})
    lba_full_melt = lba_full_melt.reset_index(drop=True)
    lba_full_melt['id'] = lba_full_melt.index
    print("Melted LBA Full covering",lba_full_melt['group_id'].nunique(),'counties\n',lba_full_melt.head(5))
    # Export Cross-Sectional data
    table_name = "feat$dd_daa_c2adpt_ans_nos$cross20minus19$cnty"
    lba_full_melt.to_sql(table_name,con=engine,if_exists="replace",index=False,dtype={
      'id':sqlalchemy.types.INTEGER(),
      'group_id':sqlalchemy.types.INTEGER(),
      'feat':sqlalchemy.types.VARCHAR(length=25),
      'group_norm':sqlalchemy.types.FLOAT(precision=53)})
    with engine.connect() as con:
      con.execute('ALTER TABLE `{}` ADD PRIMARY KEY (`id`);'.format(table_name))
      con.execute('ALTER TABLE `{}` engine=myisam;'.format(table_name))
      con.execute('CREATE INDEX gdex ON `{}` (group_id);'.format(table_name))
      con.execute('CREATE INDEX fdex ON `{}` (feat);'.format(table_name))

    # Generate an out MySQL table for the interoutcome section of the group_ids
    with engine.connect() as con:
      table_name = "county_PESH_2020_intersect"
      con.execute('DROP TABLE IF EXISTS `{}`;'.format(table_name))
      con.execute('CREATE TABLE `{}` AS SELECT * FROM `county_PESH_2020` WHERE perc_republican_president_2016 IS NOT NULL AND PercDisconnectedYouth_chr20 IS NOT NULL AND cnty IN (SELECT group_id from feat$gallup_covid$cross20$cnty) AND cnty IN (SELECT group_id from feat$dd_daa_c2adpt_ans_nos$cross20minus19$cnty);'.format(table_name))

    # Prepare Fixed Effects Data
    gallup_cols = ['WEC_sadF', 'WEB_worryF'] + ['yearweek_cnty']
    data = gallup[gallup_cols].merge(lba_full, on='yearweek_cnty')
    print("\nMerged LBA and Gallup\n",data.head())
    print('N =',len(data),'covering',data['cnty'].nunique(),'counties and',data['yearweek'].nunique(),'weeks');

    # Filter to counties with full coverage of weeks
    counties_full_coverage = data['cnty'].value_counts()[data['cnty'].value_counts() == data['yearweek'].nunique()].index.to_list()
    data = data[data['cnty'].isin(counties_full_coverage)]
    print("\nEnforce Full Week Coverage\n",data.head())
    print('N =',len(data),'covering',data['cnty'].nunique(),'counties and',data['yearweek'].nunique(),'weeks');

    outcomes = ['avg_dep','avg_anx','WEC_sadF','WEB_worryF']
    print("\nData Stats: mean (std)")
    for outcome in outcomes:
        print("-> {} \t= {} ({})".format(outcome, data[outcome].mean(), data[outcome].std()))

    # County Center All The Outcomes
    entity = "cnty"
    avg_outcomes = ["avg({})".format(x) for x in outcomes]
    mean_data = data.groupby(entity)[outcomes].mean().reset_index() # find average within each entity
    # Apply mean centering to data
    print("\nMean outcome data per {}\n{}".format(entity, mean_data.head()))
    mean_data.columns = [entity]+avg_outcomes
    county_centered_data = data.merge(mean_data, on=entity) # merge on the average values
    county_centered_data[outcomes] = county_centered_data[outcomes] - county_centered_data[avg_outcomes].values # subtract off the average values
    county_centered_data = county_centered_data.drop(columns=avg_outcomes)

    # standardizing the county centered cols
    # cols_to_std = []
    # cols_to_std = ['WEB_worryF','WEC_sadF','avg_anx','avg_dep']
    # for col in cols_to_std:
    #   county_centered_data[col] = county_centered_data[col] / county_centered_data[col].std()
    #   print("county_centered_data['{}'].std() =".format(col), county_centered_data[col].std())
    # # Scale results to [0,1]
    # min_max_scaler = MinMaxScaler()
    # county_centered_data[cols_to_std] = min_max_scaler.fit_transform(county_centered_data[cols_to_std])
    # data[cols_to_std] = min_max_scaler.fit_transform(data[cols_to_std])

    # Add month and quarter to mean centered data
    county_centered_data['month'] = pd.DatetimeIndex(county_centered_data['date']).month
    county_centered_data['quarter'] = pd.DatetimeIndex(county_centered_data['date']).quarter

    print("\nCounty Centered Data")
    county_centered_data_countyweek = county_centered_data
    df_printout(county_centered_data_countyweek,outcomes,space="cnty",time="yearweek")

    # Aggregate to County-Month
    county_centered_data_countymonth = county_centered_data.groupby(["cnty","month"]).mean().reset_index()
    print("County Month County Centered Data")
    df_printout(county_centered_data_countymonth,outcomes,space="cnty",time="month")

    # Aggregate to County-Quarter
    county_centered_data_countyquarter = county_centered_data.groupby(["cnty","quarter"]).mean().reset_index()
    print("County Quarter County Centered Data")
    df_printout(county_centered_data_countyquarter,outcomes,space="cnty",time="quarter")

    # Aggregate to Region-Week
    county_centered_data_regionweek = county_centered_data.groupby(["region_name","yearweek"]).mean().reset_index()
    print("Region Week County Centered Data")
    df_printout(county_centered_data_regionweek,outcomes,space="region_name",time="yearweek")

    # Aggregate to Region-Month
    county_centered_data_regionmonth = county_centered_data.groupby(["region_name","month"]).mean().reset_index()
    print("Region Month County Centered Data")
    df_printout(county_centered_data_regionmonth,outcomes,space="region_name",time="month")

     # Aggregate to Region-Quarter
    county_centered_data_regionquarter = county_centered_data.groupby(["region_name","quarter"]).mean().reset_index()
    print("Region Quarter County Centered Data")
    df_printout(county_centered_data_regionquarter,outcomes,space="region_name",time="quarter")

    # Aggregate to National-Week
    county_centered_data_nationalweek = county_centered_data.groupby(["yearweek"]).mean().reset_index()
    print("National Week County Centered Data")
    df_printout(county_centered_data_nationalweek,outcomes,space=None,time="yearweek")

    # Aggregate to County-Year
    county_centered_data_countyyear = data.groupby(["cnty"]).mean().reset_index()
    print("County Year County Centered Data")
    df_printout(county_centered_data_countyyear,outcomes,space="cnty",time=None)
    
    # TODO Using an aggregate?
    county_centered_data = county_centered_data_nationalweek

    print("Data Being Used:")
    print(county_centered_data.describe())

    # Run OLS Model
    formulas = []
    formulas.append("WEC_sadF ~ avg_dep ") 
    formulas.append("WEB_worryF ~ avg_anx ") 
    formulas.append("WEC_sadF ~ avg_anx " )
    formulas.append("WEB_worryF ~ avg_dep ") 
    formulas.append("avg_dep ~ WEC_sadF ") 
    formulas.append("avg_anx ~ WEB_worryF ") 
    formulas.append("avg_anx ~ WEC_sadF " )
    formulas.append("avg_dep ~ WEB_worryF ") 
    
    for formula in formulas:
      print('\t\tCounty Centered OLS on',formula)
      mod = smf.ols(formula, data=county_centered_data).fit()
      #print(mod.summary().tables[0])
      print(mod.summary().tables[1])


    # Run Mixed Linear Model
    if "region_name" in list(data.columns):
      for formula in formulas[:1]:
        md = smf.mixedlm(formula=formula, data=data, groups=data["region_name"])
        mdf = md.fit()
        print('\n',mdf.summary())


    # Run correlations (can be done on any level)
    group_on = 'yearweek_cnty' # yearweek = week x national, yearweek_cnty = week x county, yearweek_state = week x state, cnty = year x county
    merge = data.groupby(group_on).mean().reset_index()
    print("\nAggregated Data for Correlation\n",merge.head(5),'\n...')
    corr = merge.corr(method="pearson")
    print("\nLBA vs Gallup COVID by",group_on,'\n', corr)
    print(len(merge),"samples used for correlation")
    corr_plot = sns.heatmap(corr.head(2), center=0, square=True, linewidths=.5, annot=True)
    corr_plot.figure.savefig("LBA vs Gallup County Centered Data.png", bbox_inches='tight')