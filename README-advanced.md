# Robust Meausurements of Mental Health in Space and Time

[Return to simple instructions](/README.md)

## **Advanced options**: run each step independently:

### 1. Tweet Gathering

### 2. Data Preparation
  - Filter to english tweets
    - `cd hadoop-tools/languageFilter`
    - `./runThis.sh <path/to/input> <path/to/output> MESSAGE_FIELD en`
    - ex. `./runThis.sh /hadoop_data/ctlb/2020/timelines2020.csv /hadoop_data/ctlb/2020/english/timelines2020.csv 2 en`
  - Filter out retweets, tweets with URLs, and duplicates
    - `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/deduplicate_and_filter.py --input_file </path/to/input> --output_file </path/to/output> --message_field <column number> --group_field <column number>`
    - ex. `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/deduplicate_and_filter.py --input_file /hadoop_data/ctlb/2020/english/timelines2019.csv --output_file /hadoop_data/ctlb/2020/dedup/timelines2019_en.csv --message_field 2 --group_field 0`

### 3. Account-level, time scoring   
  - Word extraction
    - `cd hadoop-tools/nGramExtraction/`
    - `./runThis INPUT OUTPUT MESSAGE_FIELD GROUP_ID N` 
      - GROUP_ID must be "time_unit:account_id"
      - ex.`./runThis.sh /hadoop_data/ctlb/2020/feats/timelines2020_full_3upts.csv /hadoop_data/ctlb/2020/feats/feat.1gram.timelines2020_full_3upts.yw_user_id 3 0 1`
      - ex. outputs `/hadoop_data/ctlb/2020/feats/feat.1gram.timelines2020_full_3upts.yw_user_id`
  - Reweight users for location representativeness (uses poststratification weights)
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input </hadoop/path/input> --output </hadoop/path/output> --mapping_file </hadoop/path/mapping.csv>`
      - ex. - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2020_full_3upts.yw_user_id --output /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id --mapping_file /home/smangalik/post_strat_weights/users_2020/yw_user_2020_weights_income_k10_mbn50.csv`
      - ex. outputs `/hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id`
  - Reset outlier words usage per account
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_scale`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id --no_scale`
      - ex. outputs `/hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts3sig.yw_user_id`
  - Generate weighted scores for users based on wellbeing lexicon
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file <lex_file.csv> --word_table <hadoop/path/1gram> --output_file </hadoop/path/output>`
      - ex. `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxLex_ctlb2adapt_nostd.csv --word_table /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts3sig.yw_user_id --output_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts3sig.yw_user_id`
      - ex. outputs `/hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts3sig.yw_user_id`
  - Rescale scores
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_sigma`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts3sig.yw_user_id --no_sigma`
      - ex. outputs `/hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_05scale.timelines2020_full_3upts.yw_user_id`

### 4. Location aggregation  
- Add higher space level, e.g. add counties column
  - `~/spark/bin/spark-submit /hadoop-tools/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar ~/hadoop-tools/sparkScripts/add_county_to_feat_tables-ctlb.py --word_table <input> --mapping_file <agg_mapping>`
- Aggregate  
  - `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file </hadoop/path/input> --output_file /<hadoop/path/output>  --gft <selected_threshold>`
    - ex. `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file /hadoop/path/1gram.yw_user_id.cnty --output_file /hadoop_data/ctlb/2020/feats/feat.1gram.yw_cnty --gft 200`


### 5. Analysis (Optional) 


