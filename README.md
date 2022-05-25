# Robust Meausurements of Mental Health in Space and Time
Code for robust spatial and temporal measurements of well-being. 

This system is currently built around aggregation to the account (user) level and the weekly level and then aggregating those results up to the county and week level. The code is generally flexible to other space and time resolutions but has not been tested and may need adjustments.

## Full pipeline for generating scores and running analyses:

### Prerequisites:
- create a symlink between your home directory and your spark directory called `spark/` such that spark jobs be run via `~/spark/bin/spark-submit`

### 1. Tweet Gathering
  - Collecting tweets
    - Use the [TwitterMySQL](https://github.com/dlatk/TwitterMySQL) library found in DLATK, the GitHub repository has information about accessing the Twitter API keys to pull tweets
    - Command to run tweet extraction: `python twInterface.py -d twitter -t batch_1 --time_lines --user_list </path/to/listOfAccounts.txt> --authJSON </path/to/twitterAPIkeys.json>`
  - Mapping twitter users to counties
    - Further details can be found in: Characterizing [Geographic Variation in Well-Being Using Tweets](https://www.researchgate.net/publication/282330246_Characterizing_Geographic_Variation_in_Well-Being_Using_Tweets)

### 2. Data Preparation
Filters to english, as well as removes retweets, tweets with urls, duplicate tweets. 
- Runnable as: `./filterTweets.sh HDFS_INPUT_CSV MESSAGE_FIELD_DSX GROUP_FIELD_IDX` 
  - Input: 
      -  `HDFS_INPUT_CSV` -- tweets in csv 
      - `message_field_idx` -- the column index for the text
      - `group_field_idx` -- is the column index for the id. 
  - Output: 
    - `hdfs_intput_csv_english_deduped` -- same format as input but with rows filtered out

- **Advanced options:** run each step independently: 
  - Filter to english tweets
    - `./runThis.sh <path/to/input> <path/to/output> MESSAGE_FIELD en`
    - ex. `./runThis.sh /hadoop_data/ctlb/2020/timelines2020.csv /hadoop_data/ctlb/2020/english/timelines2020.csv 2 en`
  - Filter out retweets, tweets with URLs, and duplicates
    - `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/deduplicate_and_filter.py --input_file </path/to/input> --output_file </path/to/output> --message_field <column number> --group_field <column number>`
    - ex. `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/deduplicate_and_filter.py --input_file /hadoop_data/ctlb/2020/english/timelines2019.csv --output_file /hadoop_data/ctlb/2020/dedup/timelines2019_en.csv --message_field 2 --group_field 0`

### 3. Account-level, time scoring   
  Extracts word mentions per group_id (group_id is commonly `time_unit:user_id`), optionally postratifies given weights_csv, and then runs the specified weighted lexicon per group_id. 
- Runnable as: `lexiconExtractAndScore.sh HDFS_INPUT_CSV MESSAGE_FIELD_IDX GROUP_ID_IDX LEXICON_CSV [WEIGHTS_CSV]`
  - Input:
    - `HDFS_INPUT_CSV` -- social media data (e.g. tweets) with tweet body, group_id containing account number of tweeter and timeunit (like week) tweet was made, mapping for accounts to a location (like county)
    - `MESSAGE_FIELD_IDX` -- column index where message (text) is contained
    - `GROUP_ID_IDX` -- column index for the group id (i.e. the column to agrgegate words by). 
    - `LEXICON_CSV` -- weighted lexicon: column 0 is word, column 1 is category, and column 3 is weight (TODO: link to lexica)
    - `WEIGHTS_CSV` -- (optional) maps group_id to weights: column 0 is group_id, column 1 is weight
  - If accounts are going to be reweighted then a mapping between entities and weights must be provided
  - Output data format: `[timeunit+account], [score_type], [weighted_count], [weighted_score]`


- **Advanced options:** run each step independently. 
  - Word extraction
    - `cd hadoop-tools/nGramExtraction/`
    - `./runThis INPUT OUTPUT MESSAGE_FIELD GROUP_ID N` 
      - GROUP_ID must be "time_unit:account_id"
      - ex.`./runThis.sh /hadoop_data/ctlb/2020/feats/timelines2020_full_3upts.csv /hadoop_data/ctlb/2020/feats/feat.1gram.timelines2020_full_3upts.yw_user_id 3 0 1`
  - Reweight users for location representativeness
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input </hadoop/path/input> --output </hadoop/path/output> --mapping_file </hadoop/path/mapping.csv>`
      - ex. - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/reweight_userid_feats.py --input /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2020_full_3upts.yw_user_id --output /hadoop_data/ctlb/2020/feats/feat.dd_depAnxLex_ctlb2_weighted.timelines2020_full_3upts.yw_user_id --mapping_file /home/smangalik/post_strat_weights/users_2020/yw_user_2020_weights_income_k10_mbn50.csv`
  - Reset outlier words usage per account
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_scale`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_1upts_100users.yw_cnty --no_scale`
  - Generate weighted scores for users based on wellbeing lexicon
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file <lex_file.csv> --word_table <hadoop/path/1gram> --output_file </hadoop/path/output>`
      - ex. `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/topics_extraction.py --lex_file /home/smangalik/hadoop-tools/permaLexicon/dd_depAnxLex_ctlb2adapt_nostd.csv --word_table /hadoop_data/ctlb/2019/feats/feat.1gram.timelines2019_full_3upts.yw_user_id --output_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id`
  - Rescale scores
    - `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file </hadoop/path/input> --no_sigma`
      - ex `~/spark/bin/spark-submit ~/hadoop-tools/sparkScripts/outlier_reset.py --input_file /hadoop_data/ctlb/2019/feats/feat.dd_depAnxLex_ctlb2_nostd.timelines2019_full_3upts.yw_user_id --no_sigma`

### 4. Location aggregation  
  Aggregates lexica to a higher order group by field (e.g. county)
- Runnable as `./locationTimeAggregation.sh HDFS_INPUT_CSV GROUP_ID_IDX AGG_CSV MIN_GROUPS [HDFS_OUTPUT_CSV]`
  - `HDFS_INPUT_CSV` -- csv with group_ids and scores per group_id
  - `GROUP_ID_IDX` -- column index for the group id (i.e. the column to be mapped to a higher order group)
  - `AGG_CSV` -- csv with mapping from group_id (column 0) to higher order group (column 1): e.g. mapping from `time_unit:user_id` to `time_unit:county_id`. 
  - `MIN_GROUPS` -- Minimum number of groups per higher order group in order to include in aggregated output. 
  - `HDFS_OUTPUT_CSV` -- (optionally) specify the output csv filename (otherwise it will be INPUT_CSV-AGG_CSV)


- **Advanced Options**: Run each step individually. 
  - Aggregation to higher space and/or time
  - `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file </hadoop/path/input> --output_file /<hadoop/path/output>  --gft <selected_threshold>`
    - ex. `~/spark/bin/spark-submit  ~/hadoop-tools/sparkScripts/agg_feats_to_group.py --input_file /hadoop/path/1gram.yw_user_id.cnty --output_file /hadoop_data/ctlb/2020/feats/feat.1gram.yw_cnty --gft 200`

### You now have the scores per time:unit, community. 

### 5. Analysis (Optional) 
- **First, move your scores to mysql**
  - Steps to do this. 
  - TODO: consider creating a "select.sql" that is commonly used for everything below. 
- **A. Generate Reliability Scores**
  Can be used to evaluate the 1-cohen's d reliability of any measure stored in MySQL 
  - edit `scripts/cohensdh.py` and change `get_county_feats()` of the scripts replace `sql = ...` with a SQL query that returns your generated scores in the format `<timeunit>:<spaceunit>, <DEP_SCORE>, <count based score>, <group norm based score>` This will generate a JSON file that stores a compact representation of the data used by these analysis scripts, this JSON is only created once to save time reading from SQL
  - `python scripts/cohensdh.py`
- **B. Plotting scores over time**
  - do the first step of A except to `scripts/feat_over_time.py` Functions are provided to generate a time series plot for any collection of space units.
  - `python scripts/feat_over_time.py`
- **C. Convergent Validity**
  - Allows for a set of patsy-style modelss to be evaluated
  - `python scripts/fixed_effects.py`
- **D. External Criteria // get from Nikita** 
  - Space `scripts/dlatk_space.sh` 
  - Time `scripts/dlatk_time.sh`


