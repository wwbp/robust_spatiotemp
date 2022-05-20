# Robust Meausurements of Mental Health in Space and Time
Code for robust spatial and temporal measurements of well-being. 

This system is currently built around aggregation to the account (user) level and the weekly level and then aggregating those results up to the county and week level. The code is generally flexible to other space and time resolutions but has not been tested and may need adjustments.

## Full pipeline for generating scores and running analyses:

### 1. Tweet gathering //ask Sal to provide example command
  - link to how to map twitter users to counties
  - [gatherTweets.sh user_ids.csv hdfs:/data/tweet_storage/]
  - link to nested README

### 2. Data Preparation
- Runnable as: `[filterTweets.sh 'hdfs:/data/tweet_storage/' -> 'hdfs:/data/tweet_storage/filtered']`  //priority: 4
  - Data format:


### 3. Account-level, time scoring   
- Runnable as: `lexiconExtractAndScore.sh 'hdfs:/data/tweet_storage/filtered/' 'hdfs:/data/tweet_storage/scores'` //priority: 1
  - Input tweets must include: tweet body, account number of tweeter, datetime tweet was made, mapping for accounts to a location (like county)
  - If accounts are going to be reweighted then a mapping between entities and weights must be provided
  - Output data format: `[timeunit+account], [score_type], [weighted_count], [weighted_score]`
- Intermediate steps performed
  - Word extraction
    - `cd hadoop-tools/nGramExtraction/`
    - `./runThis INPUT OUTPUT MESSAGE_FIELD GROUP_ID N` 
      - ex.`./runThis.sh /hadoop_data/ctlb/2020/feats/timelines2020_full_3upts.csv /hadoop_data/ctlb/2020/feats/feat.1gram.timelines2020_full_3upts.yw_user_id 3 0 1`
  - Reweight users for location representativeness
    - `spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input </hadoop/path/input> --output </hadoop/path/output> --mapping_file </hadoop/path/mapping.csv>`
      - ex. - `spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2020_full_3upts.yw_user_id --output /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id --mapping_file /home/smangalik/post_strat_weights/users_2020/yw_user_2020_weights_income_k10_mbn50.csv`
  - Remove outlier words by account
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_scale`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty --no_scale`
  - Generate weighted scores for users based on wellbeing lexicon
    - `~/spark/bin/spark-submit hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file <lex_file.csv> --word_table <hadoop/path/1gram> --output_file </hadoop/path/output>`
      - ex. `~/spark/bin/spark-submit hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxLex_ctlb2adapt_nostd.csv --word_table /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id --output_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id`
  - Rescale scores
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_sigma`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file hadoop_data/ctlb/2020/feats/feat.met_a30_2000_cp.timelines2020_en_dedup_3upts.yw_cnty.csv --no_sigma`

### 4. Location aggregation  
- Runnable as `locationTimeAggregation.sh user_id_location.csv '/data/tweet_storage/scores/' '/data/tweet_storage/locationtime/'`  //priority: 2
- Intermediate steps performed
  - Aggregation to higher space and/or time
  - `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file </hadoop/path/input> --output_file /<hadoop/path/output>  --gft <selected_threshold>`
    - ex. `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file /hadoop/path/1gram.yw_user_id.cnty --output_file /hadoop_data/ctlb/2020/feats/feat.1gram.yw_cnty --gft 200`

### 5. Analysis //script per analysis [main page at least does plotting)
 - Plotting feat_over_time.py
 - reliability reliability.py
 - convergent validity fixed_effects.py   //priority: 3
  - external criteria dlatk.sh
    - Space
    - Time

OUTPUT: Plots over time (with given location_id) 
