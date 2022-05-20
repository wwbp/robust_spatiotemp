# Robust Meausurements of Mental Health in Space and Time
Code for robust spatial and temporal measurements of well-being. 

## Full pipeline for generating scores and running analyses:

1. Tweet gathering //ask Sal to provide example command
  - link to how to map twitter users to counties
  - [gatherTweets.sh user_ids.csv hdfs:/data/tweet_storage/]
  - link to nested README
2. Data filtering
- [filterTweets.sh 'hdfs:/data/tweet_storage/' -> 'hdfs:/data/tweet_storage/filtered']  //priority: 4
link to nested README

3. Account-level, time scoring   
- word extraction
- weighted lexicon scoring 
- [lexiconExtractAndScore.sh 'hdfs:/data/tweet_storage/filtered/' 'hdfs:/data/tweet_storage/scores']   //priority: 1
- link to nested README  

4. Location aggregation  
- Reweighting
- Aggregation within thresholds
- [locationTimeAggregation.sh user_id_location.csv week 'hdfs:/data/tweet_storage/scores/' 'hdfs:/data/tweet_storage/locationtime/']  //priority: 2
link to nested README

5. Analysis //script per analysis [main page at least does plotting)
 - Plotting feat_over_time.py
 - reliability reliability.py
 - convergent validity fixed_effects.py   //priority: 3
  - external criteria dlatk.sh
    - Space
    - Time

OUTPUT: Plots over time (with given location_id) 
