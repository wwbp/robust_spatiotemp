"""
run as `python3 feat_over_time.py`
"""

import os, time, json, datetime, sys

if os.environ.get('DISPLAY','') == '':
  import matplotlib
  print('no display found. Using non-interactive Agg backend')
  matplotlib.use('Agg')

from cycler import cycler

from pymysql import cursors, connect
from tqdm import tqdm
from datetime import timedelta

import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf
import statsmodels as sm

from utils import yearweek_to_dates, date_to_yearweek

from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Set colors and line styles
dep_blue = '#1f77b4'
anx_orange = '#ff7f0e'
blue_scale = ['#89cff0','#00ffef','#00b7eb','#6495ed','#007fff','4666ff']
orange_scale = ['#ffae42','#ffa500','#ff8c00','#ff6700','#ff4500','#cc5500']
simple_blue = ['darkviolet','blue','mediumslateblue','skyblue','black']
simple_orange = ['sandybrown','gold','red','darkorange','darkred','black']
markers = ['s','*','+','x','--','o']
linestyles = ['--','-.',':','dotted','-']

# Open default connection
print('Connecting to MySQL...')
connection  = connect(read_default_file="~/.my.cnf")

# Get supplemental data
county_info = pd.read_csv("county_fips_data.csv",encoding = "utf-8")
county_info['cnty'] = county_info['fips'].astype(str).str.zfill(5)

# Iterate over all time units to create county_feats[county][year_week][DEP_SCORE] = feats
def get_county_feats(cursor, table_years):
  county_feats = {}
  for table_year in table_years:
    #sql = "select * from ctlb2.feat$dd_depAnxAng$timelines{}$yw_cnty$1gra;".format(table_year) # unweighted
    #sql = "select * from ctlb2.feat$dd_depAnxAng_rw$timelines{}$3upt100user$yw_cnty$1gra;".format(table_year) # reweighted 100 users for GFT
    #sql = "select * from ctlb2.feat$dd_depAnxAng_rw$timelines{}$3upt3_user$yw_cnty$1gra;".format(table_year) # reweighted 30 users for GFT
    
    #sql = "select * from ctlb2.feat$dd_depAnxLex_std$timelines{}$yw_cnty;".format(table_year) # standardized test
    #sql = "select * from ctlb2.feat$dd_depAnxLex_nostd$timelines{}$yw_cnty;".format(table_year) # non-standardized test
    #sql = "select * from ctlb2.feat$dd_depAnxLex_nofs$timelines{}$yw_cnty;".format(table_year) # no feature selection test

    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines{}$3upt50user$yw_cnty;".format(table_year)       # 50  GFT yw_cnty
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines{}$3upt500user$yw_cnty;".format(table_year)      # 500 GFT yw_cnty
    # sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines{}$3upt300user$yw_supercnty;".format(table_year) # 300 GFT yw_supercnty

    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$3upt50user$yw_cnty;"       # 50  GFT yw_cnty 19to20
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$3upt200user$yw_cnty;"      # 200 GFT yw_cnty 19to20
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$3upt500user$yw_cnty;"      # 500 GFT yw_cnty 19to20
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$3upt300user$yw_supercnty;" # 300 GFT yw_supercnty 19to20
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$noscale200user$yw_cnty;" # 200 GFT yw_cnty 19to20 (not scaled)
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$05sc6sig200user$yw_cnty;" # 6 sigma outliers

    sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$05sc50user$yw_cnty;" # 50 GTF keep outliers
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$05sc200user$yw_cnty;" # 200 GTF keep outliers
    #sql = "select * from ctlb2.feat$dd_depAnxLex_ctlb2$timelines19to20$05sc500user$yw_cnty;" # 500 GTF keep outliers
    

    print('Processing {}'.format(sql))

    cursor.execute(sql)

    for result in tqdm(cursor.fetchall_unbuffered()): # Read _unbuffered() to save memory

      yw_county, feat, value, value_norm = result[0], result[1], result[2], result[3]

      if feat == '_int': continue
      yearweek, county = yw_county.split(":")
      if county == "" or yearweek == "": continue # skip corrupted values
      county = str(county).zfill(5)

      # Store county_feats
      if county_feats.get(county) is None:
        county_feats[county] = {}
      if county_feats[county].get(yearweek) is None:
        county_feats[county][yearweek] = {}
      county_feats[county][yearweek][feat] = value_norm

  return county_feats

def county_list_to_df(county_list):
  # Get all available year weeks in order
  available_yws = []
  for county in county_list:
      available_yws.extend( list(county_feats[county].keys()) )
  available_yws = list(set(available_yws))
  available_yws.sort()
  min_yw = available_yws.pop(0)

  # Get feature scores over time
  # yw_anx_score[yearweek] = [ all anx_scores... ]
  yw_anx_score = {}
  yw_dep_score = {}

  for county in county_list:
      yearweeks = list(county_feats[county].keys())
      for yearweek in yearweeks:
          # skip the minimum possible yw
          if yearweek == min_yw:
            continue

          # Add anxiety scores
          if yearweek not in yw_anx_score.keys():
              yw_anx_score[yearweek] = []
          yw_anx_score[yearweek].append( county_feats[county][yearweek]['ANX_SCORE'] )

          # Add depression scores
          if yearweek not in yw_dep_score.keys():
              yw_dep_score[yearweek] = []
          yw_dep_score[yearweek].append( county_feats[county][yearweek]['DEP_SCORE'] )

  # Store results
  columns = ['date','yearweek','avg_anx','avg_dep','std_anx','std_dep','n']
  df = pd.DataFrame(columns=columns)

  for yw in available_yws:
      monday, thursday, sunday = yearweek_to_dates(yw)

      avg_anx = np.mean(yw_anx_score[yw])
      avg_dep = np.mean(yw_dep_score[yw])
      n = float(min(len(yw_anx_score[yw]), len(yw_dep_score[yw])))
      std_anx = np.std(yw_anx_score[yw])
      std_dep =  np.std(yw_dep_score[yw])

      df2 = pd.DataFrame([[thursday, yw, avg_anx, avg_dep, std_anx, std_dep, n]], columns=columns)
      df = df.append(df2, ignore_index = True)

  # GROUP BY if necessary
  df.set_index('date', inplace=True)
  #df = df.groupby(pd.Grouper(freq='Q')).mean() # Q = Quarterly, M = Monthly

  # Calculate columns
  df['ci_anx'] = df['std_anx'] / df['n']**(0.5) # remove sqrt for std-dev
  df['ci_dep'] = df['std_dep'] / df['n']**(0.5) # remove sqrt for std-dev
  df['ci_anx_up'] = df['avg_anx'] + df['ci_anx']
  df['ci_anx_down'] = df['avg_anx'] - df['ci_anx']
  df['ci_dep_up'] = df['avg_dep'] + df['ci_dep']
  df['ci_dep_down'] = df['avg_dep'] - df['ci_dep']

  return df

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
          "date":thursday,
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
  df = pd.merge(df,county_info[['cnty','state_name']],on='cnty')
  df['yearweek_state'] = df['yearweek'] + ":" + df['state_name']

  # GROUP BY if necessary
  #df.set_index('date', inplace=True)
  #df = df.groupby(pd.Grouper(freq='Q')).mean() # Q = Quarterly, M = Monthly

  return df

def plot_depression(counties_of_interest:list, counties_name:str, stderr=True, date_range=None, \
  color=dep_blue, alpha=1.0, linestyle=None, marker=None):
  counties_of_interest = list(set(county_feats.keys() ) & set(counties_of_interest))
  df = county_list_to_df(counties_of_interest)

  if date_range:
    start, end = date_range
    df = df.loc[start:end]

  x = df.index.tolist()
  label = 'Avg Depression ' + counties_name
  print("Plotting",label)
  ax.plot(x, df['avg_dep'],  label=label, color=color, alpha=alpha, linestyle=linestyle, marker=marker)
  ax.axes.yaxis.set_ticks([])
  if stderr:
    ax.errorbar(x, df['avg_dep'], df['ci_dep_up']-df['avg_dep'], linestyle='None', label='_nolegend_', color='b',alpha=0.5) # error bars
    n = np.sqrt(len(counties_of_interest))
    ax.fill_between(x, df['ci_dep_down'], df['ci_dep_up'], color=color, alpha=0.3*alpha) # error area

  # Label line with text
  #ax.annotate("\n".join(label.split()), xy=(x[0] - timedelta(days=7), df['avg_dep'].to_list()[0]), color=color)

def plot_anxiety(counties_of_interest, counties_name, stderr=True, date_range=None, \
  color=anx_orange, alpha=1.0, linestyle=None, marker=None):
  counties_of_interest = list(set(county_feats.keys() ) & set(counties_of_interest))
  df = county_list_to_df(counties_of_interest)

  if date_range:
    start, end = date_range
    df = df.loc[start:end]

  x = df.index.tolist()
  label = 'Avg Anxiety ' + counties_name
  print("Plotting",label)
  ax.plot(x, df['avg_anx'],  label=label, color=color, alpha=alpha, linestyle=linestyle, marker=marker)
  ax.axes.yaxis.set_ticks([])
  if stderr:
    ax.errorbar(x, df['avg_anx'], df['ci_anx_up']-df['avg_anx'], linestyle='None', label='_nolegend_', color='r',alpha=0.5) # error bars
    ax.fill_between(x, df['ci_anx_down'].tolist(), df['ci_anx_up'].tolist(), color=color, alpha=0.3*alpha) # error area

  # Label line with text
  #ax.annotate("\n".join(label.split()), xy=(x[0] - timedelta(days=7), df['avg_anx'].to_list()[0]), color=color)

def plot_events(text=True):
  events_dict = {}
  events_dict["Christmas 2019"] = dt.datetime(2019, 12, 25)
  events_dict["First US Case COVID"] = dt.datetime(2020, 1, 21) 
  #events_dict["First US COVID Death"] = dt.datetime(2020, 2, 29)
  events_dict["Pandemic Declared"] = dt.datetime(2020, 3, 11)
  events_dict["Murder of George Floyd"] = dt.datetime(2020, 5, 25)
  events_dict["Supreme Court Turnover"] = dt.datetime(2020, 9, 26) # death of RBG, amy coney barret
  #events_dict["Nomination of Amy Coney Barrett"] = [dt.datetime(2020, 9, 26), dt.datetime(2020, 10, 1)]
  #events_dict["Trump Tests Positive"] = [dt.datetime(2020, 10, 1), dt.datetime(2020, 10, 6)]
  events_dict["Election Results"] = dt.datetime(2020, 11, 7)
  #events_dict["Texas Shootings"] = [dt.datetime(2019, 8, 3), dt.datetime(2019, 8, 31)]
  bottom, top = ax.get_ylim()
  for event_name, date in events_dict.items():
    yw = date_to_yearweek(date)
    monday, thursday, sunday = yearweek_to_dates(yw)
    start_date = monday - timedelta(days=3)
    end_date = start_date + timedelta(days=1)
    ax.axvspan(start_date, end_date, alpha=0.2, color='g', label='_nolegend_') # Plot line
    if text:
      plt.text(start_date- timedelta(days=9), top, s="   {}  ".format(event_name), rotation=270, verticalalignment='top') # Line text


with connection:
  with connection.cursor(cursors.SSCursor) as cursor:
    print('Connected to',connection.host)

    # Get county feat information,
    # county_feats['01087'] = {'2020_34': {'DEP_SCORE': 0.2791998298997296, 'ANX_SCORE': 1.759353260415711}}}
    #county_feats_json = "/data/smangalik/county_feats_ctlb_100user.json" # /data/smangalik/county_feats_ctlb_xx0user.json
    
    #county_feats_json = "/data/smangalik/county_feats_ctlb_nostd.json" # 500 GFT, unscaled

    #county_feats_json = "/data/smangalik/feat_depAnxLex_3upt50user_ywcnty.json"       # 50 GFT
    #county_feats_json = "/data/smangalik/feat_depAnxLex_3upt500user_ywcnty.json"      # 500 GFT
    #county_feats_json = "/data/smangalik/feat_depAnxLex_3upt300user_superywcnty.json" # 300 GFT, Supercounty

    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt50user_ywcnty.json"       # 50 GFT 2019-2020
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user_ywcnty.json"      # 200 GFT 2019-2020
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt500user_ywcnty.json"      # 500 GFT 2019-2020
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt300user_ywsupercnty.json" # 300 GFT, Supercounty 2019-2020
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user_ywcnty_notscaled.json" # 200 GFT, notscaled 2019-2020
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user05scale6sig_ywcnty.json" # 6 sigma outliers

    county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt50user05scale_ywcnty.json"  #  50GFT keep outliers
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt200user05scale_ywcnty.json" # 200GFT keep outliers
    #county_feats_json = "/data/smangalik/feat_depAnxLex_19to20_3upt500user05scale_ywcnty.json" # 500GFT keep outliers
    

    print("Running on",county_feats_json)
    if not os.path.isfile(county_feats_json):
        #table_years = list(range(2012, 2017))
        #table_years = [2019,2020] 
        table_years = [2020]
        county_feats = get_county_feats(cursor,table_years)
        with open(county_feats_json,"w") as json_file: json.dump(county_feats,json_file)
    print("\nImporting produced county features")
    with open(county_feats_json) as json_file:
        county_feats = json.load(json_file)
    print("Import complete\n")

    # Limit county list to one state/region/division at a time
    ny_counties = county_info.loc[county_info['state_abbr'] == "NY", 'cnty'].tolist()
    al_counties = county_info.loc[county_info['state_abbr'] == "AL", 'cnty'].tolist()
    ca_counties = county_info.loc[county_info['state_abbr'] == "CA", 'cnty'].tolist()
    tx_counties = county_info.loc[county_info['state_abbr'] == "TX", 'cnty'].tolist()
    oh_counties = county_info.loc[county_info['state_abbr'] == "OH", 'cnty'].tolist()
    r1_counties = county_info.loc[county_info['region'] == 1, 'cnty'].tolist() # North East
    r2_counties = county_info.loc[county_info['region'] == 2, 'cnty'].tolist() # Midwest
    r3_counties = county_info.loc[county_info['region'] == 3, 'cnty'].tolist() # South
    r4_counties = county_info.loc[county_info['region'] == 4, 'cnty'].tolist() # West
    d1_counties = county_info.loc[county_info['division'] == 1, 'cnty'].tolist() # New England
    d9_counties = county_info.loc[county_info['division'] == 9, 'cnty'].tolist() # Pacific

    # American Community Type
    type_mapping = pd.read_csv("american_community_mapping.csv")
    type_mapping['cnty'] = type_mapping['cnty'].astype(str).str.zfill(5)
    type1_counties = type_mapping.loc[type_mapping['Type_Number'] == 1, 'cnty'].tolist() # Exurbs
    type2_counties = type_mapping.loc[type_mapping['Type_Number'] == 2, 'cnty'].tolist() # Graying America
    type3_counties = type_mapping.loc[type_mapping['Type_Number'] == 3, 'cnty'].tolist() # African American South
    type4_counties = type_mapping.loc[type_mapping['Type_Number'] == 4, 'cnty'].tolist() # Evangelical Hubs
    type5_counties = type_mapping.loc[type_mapping['Type_Number'] == 5, 'cnty'].tolist() # Working Class Country
    type6_counties = type_mapping.loc[type_mapping['Type_Number'] == 6, 'cnty'].tolist() # Military Posts
    type7_counties = type_mapping.loc[type_mapping['Type_Number'] == 7, 'cnty'].tolist() # Urban Suburbs
    type8_counties = type_mapping.loc[type_mapping['Type_Number'] == 8, 'cnty'].tolist() # Hispanic Centers
    type9_counties = type_mapping.loc[type_mapping['Type_Number'] == 9, 'cnty'].tolist() # Native American Lands
    type10_counties = type_mapping.loc[type_mapping['Type_Number'] == 10, 'cnty'].tolist() # Rural Middle America
    type11_counties = type_mapping.loc[type_mapping['Type_Number'] == 11, 'cnty'].tolist() # College Towns
    type12_counties = type_mapping.loc[type_mapping['Type_Number'] == 12, 'cnty'].tolist() # LDS Enclaves
    type13_counties = type_mapping.loc[type_mapping['Type_Number'] == 13, 'cnty'].tolist() # Aging Farmlands
    type14_counties = type_mapping.loc[type_mapping['Type_Number'] == 14, 'cnty'].tolist() # Big Cities
    type15_counties = type_mapping.loc[type_mapping['Type_Number'] == 15, 'cnty'].tolist() # Middle Suburbs
    community_counties = [type1_counties,type2_counties,type3_counties,type4_counties,type5_counties,type6_counties,type7_counties,\
      type8_counties,type9_counties,type10_counties,type11_counties,type12_counties,type13_counties,type14_counties,type15_counties]
    community_names = ['Exurbs','Graying America','African American South','Evangelical Hubs','Working Class Country','Military Posts',\
      'Urban Suburbs','Hispanic Centers','Native American Lands','Rural Middle America','College Towns','LDS Enclaves','Aging Farmlands',\
      'Big Cities','Middle Suburbs']
    for community_county_list,community_name in zip(community_counties,community_names):
      print("{}/{} are {}".format(len([x for x in community_county_list if x in county_feats.keys()]),len(community_county_list),community_name))



    # Top 5 counties by population
    top_pops = [['06037'],['17031'],['48201'],['04013'],['06073']] 
    top_pop_names = ['LA','Cook','Harris','Maricopa','San Diego']

    # Counties for Main Measurements
    #main_counties = [['36061'],['48113'],['54039']]
    #main_county_names = ['New York NY','Dallas TX','Kanawha WV']

    main_counties = [['36061'],['27145'],['17031'],]
    main_county_names = ['New York NY','Stearns MN','Cook']

    regions = [r1_counties,r2_counties,r3_counties,r4_counties]
    region_names = ["in the Northeast","in the Midwest","in the South","in the West"]
    #divisions = [d1_counties,d2_counties,d3_counties,d4_counties,d5_counties,d6_counties,d7_counties,d8_counties,d9_counties]

    all_counties = county_feats.keys()
    county_list = all_counties
    #county_list = list(set(county_feats.keys() ) & set(ny_counties)) # Use to select counties
    print("Counties considered:", len(county_list), "\n")

    # Process Language Based Assessments data
    df = county_list_to_df(county_list)
    print("LBA\n",df.head(10),'\n')
    lba_full = county_list_to_full_df(county_list)
    print("LBA (full)\n",lba_full.head(10),'\n')

    # Set up plot
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)

    x = df.index.tolist()

    # Plot Depression and Anxiety
    dep_line = plt.plot(x, df['avg_dep'], color=dep_blue, label='Average Depression')
    anx_line = plt.plot(x, df['avg_anx'], color=anx_orange, label='Average Anxiety')
    dep_area = plt.fill_between(x, df['ci_dep_down'].tolist(), df['ci_dep_up'].tolist(), color=dep_blue, alpha=0.3) # error area
    anx_area = plt.fill_between(x, df['ci_anx_down'].tolist(), df['ci_anx_up'].tolist(), color=anx_orange, alpha=0.3) # error area
    # Events
    plot_events()
    # Make plot pretty
    plt.title("National Depression & Anxiety Over Time")
    plt.xlabel("Date")
    plt.ylabel("Feature Score")
    #ax.axes.yaxis.set_ticks([]) # Clean up axes
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.gcf().autofmt_xdate()
    #plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    # Plot everything
    plt.savefig("over_time_depression_and_anxiety.png", bbox_inches='tight', dpi=300)

    # Depression Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)
    #plot_depression(ny_counties, "in New York")
    #plot_depression(d1_counties, "in New England")
    #plot_depression(ca_counties, "in California")
    #for i, (region,region_name) in enumerate(zip(regions,region_names)): plot_depression(region, region_name, color=simple_blue[i], linestyle=linestyles[i], stderr=False)
    #for i, (cnty,cnty_name) in enumerate(zip(main_counties,main_county_names)): plot_depression(cnty, cnty_name, color=simple_blue[i], linestyle=linestyles[i], stderr=True)
    #for i, (cnty,cnty_name) in enumerate(zip(top_pops,top_pop_names)): plot_depression(cnty, cnty_name, color=simple_blue[i], linestyle=linestyles[i], stderr=True)
    plot_depression(all_counties, "Nationally",alpha=0.4)
    plot_events()
    # Make plot pretty
    plt.title("Depression Over Time")
    plt.xlabel("Date")
    plt.ylabel("Depression Score")
    #ax.axes.yaxis.set_ticks([]) # Hide y-axis ticks
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    # Plot everything
    plt.savefig("over_time_depression.png", bbox_inches='tight', dpi=300)

    # Anxiety Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)
    #plot_anxiety(ny_counties, "in New York")
    #plot_anxiety(d1_counties, "in New England")
    #for i, (region,region_name) in enumerate(zip(regions,region_names)): plot_anxiety(region, region_name, color=simple_orange[i], linestyle=linestyles[i], stderr=False)
    #for i, (cnty,cnty_name) in enumerate(zip(main_counties,main_county_names)): plot_anxiety(cnty, cnty_name, color=simple_orange[i], linestyle=linestyles[i], stderr=True)
    plot_anxiety(all_counties, "Nationally",alpha=0.4)
    plot_events()
    # Make plot pretty
    plt.title("Anxiety Over Time")
    plt.xlabel("Date")
    plt.ylabel("Anxiety Score")
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    # Plot everything
    plt.savefig("over_time_anxiety.png", bbox_inches='tight', dpi=300)

    print("STOPPING EARLY")
    sys.exit(0)

    # Valid Counties Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)

    df = county_list_to_df(all_counties)
    x = df.index.tolist()
    plt.plot(x, df['n'], 'g-', label='# of Valid Counties')
    # Make plot pretty
    plt.title("Valid Counties Over Time")
    plt.xlabel("Date")
    plt.ylabel("Valid Counties")
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    # Plot everything
    plt.ylim(ymin=0)
    ax.set_ylim(ymin=0)
    ax.set_ylim(bottom=0)
    plt.draw()
    plt.savefig("over_time_n.png", bbox_inches='tight', dpi=300)

    # Household Pulse Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)
    ax2 = ax.twinx()
    sql = "SELECT week, avg(gen_health), avg(gad2_sum), avg(phq2_sum), std(gen_health), std(gad2_sum), std(phq2_sum), count(gen_health), count(gad2_sum), count(phq2_sum) FROM household_pulse.pulse GROUP BY week ORDER BY week asc;"
    household_pulse = pd.read_sql(sql, connection)
    household_pulse_week_mapping = pd.read_csv("./household_pulse_week_mapping.csv") # Add custom yearweeks
    household_pulse = household_pulse.merge(household_pulse_week_mapping, on='week')
    household_pulse['date'] = household_pulse['yearweek'].apply(lambda yw: yearweek_to_dates(yw)[1])
    print("\nHousehold Pulse\n")#,household_pulse.head(10))

    household_pulse = household_pulse[household_pulse['date'] < '2021-01-01'] # Trim household_pulse to 2020
    x = household_pulse['date'].tolist()
    anx_line = ax2.plot(x, household_pulse['avg(gad2_sum)'], 'b-', label='Worry and Anxiety (0 is best)')
    dep_line = ax2.plot(x, household_pulse['avg(phq2_sum)'], 'r-', label='Disinterest and Depression (0 is best)')
    gen_line = ax2.plot(x, household_pulse['avg(gen_health)'], 'g-', label='General Health (5 is best)')
    #plot_events()
    #plot_depression(all_counties, "Nationally")
    plot_anxiety(all_counties, "Nationally")
    plt.title("Baselines Over Time")
    plt.xlabel("Date")
    plt.ylabel("Feature Score")
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    plt.savefig("over_time_health_baselines.png", bbox_inches='tight', dpi=300)


    # NOT A LOT OF DATA: Gallup Micro-Poll Plot
    plt.clf()
    gallup_old = pd.read_csv("./gallup_micro_polls_weekly.csv")
    gallup_old['date'] = gallup_old['yearweek'].apply(lambda yw: yearweek_to_dates(yw)[1])
    print("\nGallup Micropoll\n")#),gallup_old.head(10))

    x = gallup_old['date'].tolist()
    pain_line = plt.plot(x, gallup_old['avg(wp68_clean)'], label='Experienced Pain? (0 is best)')
    worry_line = plt.plot(x, gallup_old['avg(wp69_clean)'], label='Experienced Worry? (0 is best)')
    stress_line = plt.plot(x, gallup_old['avg(wp71_clean)'], label='Experienced Stress? (0 is best)')
    dep_line = plt.plot(x, gallup_old['avg(H4D_clean)'], label='Depression Diagnosis? (0 is best)')
    plt.title("Baselines Over Time")
    plt.xlabel("Date")
    plt.ylabel("Feature Score")
    plt.gcf().autofmt_xdate()
    plt.legend()
    #dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    dates= list(pd.date_range('2018-11-01','2019-08-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    plt.savefig("over_time_gallup_old.png", bbox_inches='tight', dpi=300)

    # Gallup COVID Panel Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)
    ax2 = ax.twinx()
    #ax2 = plt
    sql = "select fips as cnty, yearweek, WEB_worryF, WEC_sadF, neg_affect_lowArousal, neg_affect_highArousal, neg_affect, pos_affect, affect_balance from gallup_covid_panel_micro_poll.old_hasSadBefAug17_recodedEmoRaceGenPartyAge_v3_02_15;"
    gallup_full = pd.read_sql(sql, connection)
    gallup_full = gallup_full[gallup_full['cnty'].isin(all_counties)] # filter to only counties in all_counties
    gallup_full['yearweek'] = gallup_full['yearweek'].astype(str)
    gallup_full['yearweek'] = gallup_full['yearweek'].str[:4] + "_" + gallup_full['yearweek'].str[4:]
    gallup_full = gallup_full.groupby(by=["yearweek","cnty"]).mean().reset_index() # aggregate to yearweek_cnty so we share group_id with LBA
    gallup_full['yearweek_cnty'] = gallup_full['yearweek'] + ":" + gallup_full['cnty']
    gallup_full = pd.merge(gallup_full,county_info[['cnty','state_name']],on='cnty')
    gallup_full['yearweek_state'] = gallup_full['yearweek'] + ":" + gallup_full['state_name']
    print("\nGallup COVID Panel (Full)\n",gallup_full.head(10))
    gallup_stderr = gallup_full.groupby(by=["yearweek"]).sem().reset_index() # TODO stderr is using ALL entries and not county aggregates
    gallup = gallup_full.groupby(by=["yearweek"]).mean().reset_index() # Used for plotting by yearweek
    gallup['date'] = gallup['yearweek'].apply(lambda yw: yearweek_to_dates(yw)[1])

    x = gallup['date'].tolist()
    #sad_line = ax2.plot(x, gallup['WEC_sadF'], color="blue", label='Sadness', linestyle='dashed')
    #sad_err = ax2.fill_between(x, (gallup['WEC_sadF']-gallup_stderr['WEC_sadF']).tolist(), (gallup['WEC_sadF']+gallup_stderr['WEC_sadF']).tolist(), color="blue", alpha=0.3, linestyle='None', label='_nolegend_') # error area
    worry_line = ax2.plot(x, gallup['WEB_worryF'], color='red', label='Worry', linestyle='dashed')
    worry_err = ax2.fill_between(x, (gallup['WEB_worryF']-gallup_stderr['WEB_worryF']).tolist(), (gallup['WEB_worryF']+gallup_stderr['WEC_sadF']).tolist(), color="red", alpha=0.3, linestyle='None', label='_nolegend_') # error area

    #pos_line = ax2.plot(x, gallup['pos_affect'], label='Pos Affect')
    #neg_line = ax2.plot(x, gallup['neg_affect'], label='Neg Affect')
    #low_neg_line = ax2.plot(x, gallup['neg_affect_lowArousal'], label='Low Arousal Neg Affect') # Depression
    #high_neg_line = ax2.plot(x, gallup['neg_affect_highArousal'], label='High Arousal Neg Affect') # Anxiety
    #affect_bal_line = ax2.plot(x, gallup['affect_balance'], label='Affect Balance')
    #plot_events(text=True)
    #plot_depression(all_counties, "Nationally", date_range=(dt.datetime(2020,2,28),dt.datetime(2020,8,30)))
    plot_anxiety(all_counties, "Nationally", date_range=(dt.datetime(2020,2,28),dt.datetime(2020,8,30)))
    plt.title("Gallup COVID Panel and LBA Over Time")
    plt.xlabel("Date")
    plt.ylabel("Feature Score")
    #ax.axes.yaxis.set_ticks([]) # Clean up axes
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2020-03-01','2020-010-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    plt.savefig("over_time_gallup_covid.png", bbox_inches='tight', dpi=300)

    # CDC BRFSS Plot
    plt.clf()
    fig, ax = plt.subplots(1)
    fig.set_size_inches(18, 8)
    ax2 = ax.twinx()
    brfss_files = ["/data/smangalik/BRFSS_mental_health_2019.csv","/data/smangalik/BRFSS_mental_health_2020.csv"]
    brfss = pd.concat((pd.read_csv(f) for f in brfss_files))
    print("\nCDC BRFSS\n")#,brfss.head(8))
    brfss = brfss.rename(columns={"YEARWEEK": "yearweek"})
    brfss['DATE'] = pd.to_datetime(brfss['DATE'], infer_datetime_format=True) # infer datetime
    brfss = brfss[brfss['DATE'] < '2021-01-01'] # Trim brfss to 2020
    #print('\nyearweek counts\n',brfss['yearweek'].value_counts()) # yearweek data point counts
    brfss = brfss.groupby(by=["yearweek"]).agg({'MENTHLTH':['mean','sem'],  'POORHLTH':['mean','sem'],  'ACEDEPRS':['mean','sem'],  '_MENT14D':['mean','sem']}).reset_index()
    brfss.columns = ['_'.join(col).strip() for col in brfss.columns.values]
    brfss = brfss.rename(columns={"yearweek_": "yearweek"})
    #print('\nyearweek stderr\n',brfss.head(8)) # yearweek data point counts
    #brfss = brfss.groupby(by=["yearweek"]).mean().reset_index()
    brfss['DATE'] = brfss['yearweek'].apply(lambda yw: yearweek_to_dates(yw)[1]) # replace date based on yearweek
    #print(brfss.head(8))

    x = brfss['DATE'].tolist()
    menthlth_line = ax2.plot(x, brfss['MENTHLTH_mean'], label='Mentally Unhealthy Days (0 is best)', color='r')
    menthlth_err = ax2.errorbar(x, brfss['MENTHLTH_mean'], brfss['MENTHLTH_sem'], linestyle='None', label='_nolegend_', color='r')
    poorhlth_line = ax2.plot(x, brfss['POORHLTH_mean'], label='Health Affected Activities (0 is best)',color='g')
    menthlth_err = ax2.errorbar(x, brfss['POORHLTH_mean'], brfss['POORHLTH_sem'], linestyle='None', label='_nolegend_', color='g')
    #plot_events()
    plot_depression(all_counties, "Nationally")
    #plot_anxiety(all_counties, "Nationally")
    plt.title("Baselines Over Time")
    plt.xlabel("Date")
    plt.ylabel("Days")
    plt.gcf().autofmt_xdate()
    plt.legend()
    dates= list(pd.date_range('2019-01-01','2021-01-01' , freq='1M')-pd.offsets.MonthBegin(1))
    plt.xticks(dates)
    plt.savefig("over_time_brfss.png", bbox_inches='tight', dpi=300)

    # Show correlations
    print("\n----- Correlations Against Psych Evaluations -----\n")
    lba_cols = ['avg_anx','avg_dep']
    household_cols = ['avg(gen_health)', 'avg(gad2_sum)', 'avg(phq2_sum)']
    gallup_cols = ['WEC_sadF', 'WEB_worryF'] #, 'pos_affect', 'neg_affect','neg_affect_lowArousal','neg_affect_highArousal','affect_balance']
    brfss_cols = ['MENTHLTH_mean',  'POORHLTH_mean',  'ACEDEPRS_mean', '_MENT14D_mean']
    method="pearson"

    plt.clf()
    merge = df[lba_cols+['yearweek']].merge(household_pulse[household_cols+['yearweek']], on='yearweek') # national x week
    corr = merge.corr(method=method)
    print("\nLBA vs Household Pulse by yearweek\n", corr)
    print(len(merge),"samples usable for correlation")
    if len(merge) > 0:
      corr_plot = sns.heatmap(corr, center=0, square=True, linewidths=.5, annot=True)
      corr_plot.figure.savefig("LBA vs Household Pulse.png", bbox_inches='tight', dpi=300)

    # Gallup Correlation (can be done on any level)
    plt.clf()
    group_on = 'yearweek' # yearweek = week x national, yearweek_cnty = week x county, yearweek_state = week x state, cnty = year x county
    merge = lba_full.groupby(group_on).mean().reset_index()[lba_cols+[group_on]].merge(gallup_full.groupby(group_on).mean().reset_index()[gallup_cols+[group_on]], on=group_on)
    corr = merge.corr(method=method)
    print("\nLBA vs Gallup COVID by",group_on,'\n', corr)
    print(len(merge),"samples used for correlation")
    if len(merge) > 0:
      corr_plot = sns.heatmap(corr.head(2), center=0, square=True, linewidths=.5, annot=True)
      corr_plot.figure.savefig("LBA vs Gallup.png", bbox_inches='tight', dpi=300)

    plt.clf()
    merge = df[lba_cols+['yearweek']].merge(brfss[brfss_cols+['yearweek']], on='yearweek')
    corr = merge.corr(method=method)
    print("\nLBA vs BRFSS by yearweek\n", corr)
    print(len(merge),"samples used for correlation")
    if len(merge) > 0:
      corr_plot = sns.heatmap(corr, center=0, square=True, linewidths=.5, annot=True)
      corr_plot.figure.savefig("LBA vs BRFSS.png", bbox_inches='tight', dpi=300)
