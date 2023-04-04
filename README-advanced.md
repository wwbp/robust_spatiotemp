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
  - Anscombe transform 1gram usage
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/anscombe_transform.py --input_file </hadoop/path/input>`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/anscombe_transform.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id --no_scale`
      - ex. outputs `hadoop_data/ctlb/2019/feats/featANS.1gram.timelines2019_full_3upts.yw_user_id`
  - Generate dep/anx scores for user-time based on wellbeing lexicon
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file <lex_file.csv> --word_table <hadoop/path/1gram> --output_file </hadoop/path/output>`
      - ex. `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxLex_ctlb2adapt_nostd.csv --word_table /hadoop_data/ctlb/2019/feats/featANS.1gram.timelines2019_full_3upts.yw_user_id --output_file /hadoop_data/ctlb/2019/feats/featANS.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id`
      - ex. outputs `/hadoop_data/ctlb/2019/feats/featANS.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id`

### 3.5 Adding Metadata to User-Time scores
If you do not have a post-stratified weight set you will need to generate them using the documentation in [selection-bias-pipeline.md](selection-bias-pipeline.md)

  - Add location based metadata to user-time
      - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/add_county_to_feat_tables-ctlb.py --word_table <hadoop/path/depAnxScores>`
      - ex. - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/add_county_to_feat_tables-ctlb.py --word_table /hadoop_data/ctlb/2019/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2019_lex_3upts.yw_user_id`
      - ex. outputs `/hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id.cnty`
  - Add location based post-stratified weights to user-time
      - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/add_weight_to_featcnty.py --input <hadoop/path/depAnxScores.cnty> --mapping_file <hadoop/path/mapping>`
      - ex. - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/add_weight_to_featcnty.py --input /hadoop_data/ctlb/2019/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2019_lex_3upts.yw_user_id.cnty --mapping_file /home/smangalik/post_strat_weights/users_2019/yw_user_2019_weights_income_k10_mbn50.csv`
      - ex. outputs `/hadoop_data/ctlb/2019/feats/featANS.dd_daa_c2adpt_ans_nos.timelines2019_lex_3upts.yw_user_id.cnty.wt`
  - Rescale scores
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/scale_outlier.py --input_file <hadoop/path/depAnxScores.cnty.wt> --no_sigma`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/scale_outlier.py --input_file /hadoop_data/ctlb/combined/featANS.dd_daa_c2adpt_ans_nos.timelines19to20_lex_3upts.yw_user_id.cnty.wt --no_sigma`
      - ex. outputs `/hadoop_data/ctlb/combined/featANS.dd_daa_c2adpt_ans_nos.timelines19to20_lex_3upts.yw_user_id.cnty.wt.05fc`

### 4. Location aggregation  
- Aggregate  
  - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/agg_feats_to_group.py –input-file <hadoop/path/depAnxScores.cnty.wt.05fc> –output-file <hadoop/path/depAnxScores.05fc>`
    - ex. `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/agg_feats_to_group.py –input-file /hadoop_data/ctlb/combined/featANS.dd_daa_c2adpt_ans_nos.timelines19to20_lex_3upts.yw_user_id.cnty.wt.05fc –output-file /hadoop_data/ctlb/combined/featANS.dd_daa_c2adpt_ans_nos.timelines19to20_lex_3upts.yw_cnty.wt.05fc`
    - ex. outputs `/hadoop_data/ctlb/combined/featANS.dd_daa_c2adpt_ans_nos.timelines19to20_lex_3upts.yw_cnty.wt.05fc`



