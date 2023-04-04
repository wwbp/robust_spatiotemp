# Selection Bias Pipeline

Specific for the spatio-temporal project.

## Step 1: Extract 1grams

Start on the hadoop server. Run MapReduce on the hadoop cluster to extract 1grams from tweets. Do this for both `user_id` and by `year-week + user_id`.

```sh
~/hadoopPERMA/nGramExtraction/runThis.sh /hadoop/path/to/tweets.csv /hadoop/path/to/1gram/user_id.csv 3 2 1

~/hadoopPERMA/nGramExtraction/runThis.sh /hadoop/path/to/tweets.csv /hadoop/path/to/1gram/yw_user_id.csv 3 0 1
```

## Step 2: Income Predictions

Use Spark to run the income lexical model (`dd_incomePLOS.csv` provided) to predict the income for all users using the 1grams. Do this for both the `user_id` and `year-week + user_id` 1grams.

```sh
spark-submit --conf spark.executor.memory=7G --conf spark.executorEnv.PYTHONHASHSEED=323 \
    ~/hadoopPERMA/sparkScripts/topics_extraction.py \
    --lex_file /path/to/dd_incomePLOS.csv \
    --word_table /hadoop/path/to/1grams/user_id.csv \
    --output_file /hadoop/path/to/income/predictions/user_id.csv

spark-submit --conf spark.executor.memory=7G --conf spark.executorEnv.PYTHONHASHSEED=323 
    ~/hadoopPERMA/sparkScripts/topics_extraction.py \
    --lex_file /path/to/dd_incomePLOS.csv \
    --word_table /hadoop/path/to/1grams/yw_user_id.csv \
    --output_file /hadoop/path/to/income/predictions/yw_user_id.csv
```

## Step 3: Export Income Predictions to Hercules

Export both user_id and year-week user_id predictions from the hadoop server to hercules. Change servers to hercules and the run the following commands to export over ssh. Note the `*` in the path is needed.

```sh
ssh USERNAME@hadoop.wwbp.org "hadoop fs -cat /hadoop/path/to/income/predictions/user_id.csv/*" > /hercules/path/to/income/predictions/user_id.csv

ssh USERNAME@hadoop.wwbp.org "hadoop fs -cat /hadoop/path/to/income/predictions/yw_user_id.csv/*" > /hercules/path/to/income/predictions/yw_user_id.csv
```

## Step 4: Load Income Predictions Feature Tables into MySQL

On hercules, launch `mysql` and go to the `ctlb2` database.

### Step 4.1: Create tables

Create the feature tables tables for both income predictions by `user_id` and `year_week + user_id`.

```sql
create table feat$dd_incomePLOS$timelines2020$user_id$1gra (
    group_id varchar(20),
    feat varchar(15),
    value double,
    group_norm double
);

create table feat$dd_incomePLOS$timelines2020$yw_user_id$1gra (
    group_id varchar(30),
    feat varchar(15),
    value double,
    group_norm double
);
```

### Step 4.2: Import CSVs

Load both the income predictions by `user_id` and by `year-week + user_id` into these newly created feature tables.

```sql
load data local infile '/path/to/income/predictions/user_id.csv' 
    into table feat$dd_incomePLOS$timelines2020$user_id$1gra 
    fields terminated by ',' 
    enclosed by '"' lines 
    terminated by '\n';

load data local infile '/path/to/income/predictions/yw_user_id.csv' 
    into table feat$dd_incomePLOS$timelines2020$yw_user_id$1gra 
    fields terminated by ',' 
    enclosed by '"' lines 
    terminated by '\n';
```

Check feature tables to ensure things look correct.

```sql
select * from feat$dd_incomePLOS$timelines2020$user_id$1gra limit 10;
select * from feat$dd_incomePLOS$timelines2020$yw_user_id$1gra limit 10;
```

## Step 5: Income Redistribution

We perform redistribution using only the `user_id` instead of `year-week + user_id` so we can get the user's full 1gram history.

### Step 5.1: Create table

Create the following data table which will hold the data to perform redistribution.

```sql
create table income_user_2020 (
    user_id varchar(20),
    user varchar(20),
    cnty int(5),
    income_pred double,
    income double,
    income_bin double,
    income_redist double,
    income_redist_bin double
);
```

### Step 5.2: Insert data

Insert the data from the feature tables into the data table using the following command. Any value of `-1` is empty for now and will be filled eventually.

```sql
insert into income_user_2020 
    select group_id, -1, group_norm, exp(group_norm), -1, -1, -1 
    from feat$dd_incomePLOS$timelines2020$user_id$1gra;
```

Check the data table to ensure things look correct. Most columns will be `-1` which we will populate in the next steps.

```sql
select * from income_user_2020 limit 10;
```

### Step 5.3: Populate bin field

Update the data table to populate the bins.

```sql
update income_user_2020 set income_bin = 1 where income < 10000;
update income_user_2020 set income_bin = 2 where income >= 10000 and income < 15000;
update income_user_2020 set income_bin = 3 where income >= 15000 and income < 25000;
update income_user_2020 set income_bin = 4 where income >= 25000 and income < 35000;
update income_user_2020 set income_bin = 5 where income >= 35000 and income < 50000;
update income_user_2020 set income_bin = 6 where income >= 50000 and income < 75000;
update income_user_2020 set income_bin = 7 where income >= 75000 and income < 100000;
update income_user_2020 set income_bin = 8 where income >= 100000 and income < 150000;
update income_user_2020 set income_bin = 9 where income >= 150000 and income < 200000;
update income_user_2020 set income_bin = 10 where income >= 200000;
```

### Step 5.4: Populate county field

Update the data table to populate the county for each user using the `user_county_mapping_2` table.

```sql
update income_user_2020 t1, user_county_mapping_2 t2 set t1.cnty = t2.cnty where t1.user_id = t2.user_id;
```

### Step 5.5: Python script to populate remaining fields

Run the `sql_income_redist.py` script to perform redistribution and populate the remaining fields. Note that the values of `DEFAULT_PEW_BINS` and `DEFAULT_PEW_BINS` are for 2019 and 2020 data. If using other years, adjust these numbers accordingly by the PEW estimates of that time period.

```sh
python3 sql_income_redist.py --db ctlb2 --table income_user_2020
```

### Step 5.6: Populate redistributed bin field

Update the data table to populate the redistributed bins.

```sql
update income_user_2020 set income_redist_bin = 1 where income_redist < 10000;
update income_user_2020 set income_redist_bin = 2 where income_redist >= 10000 and income_redist < 15000;
update income_user_2020 set income_redist_bin = 3 where income_redist >= 15000 and income_redist < 25000;
update income_user_2020 set income_redist_bin = 4 where income_redist >= 25000 and income_redist < 35000;
update income_user_2020 set income_redist_bin = 5 where income_redist >= 35000 and income_redist < 50000;
update income_user_2020 set income_redist_bin = 6 where income_redist >= 50000 and income_redist < 75000;
update income_user_2020 set income_redist_bin = 7 where income_redist >= 75000 and income_redist < 100000;
update income_user_2020 set income_redist_bin = 8 where income_redist >= 100000 and income_redist < 150000;
update income_user_2020 set income_redist_bin = 9 where income_redist >= 150000 and income_redist < 200000;
update income_user_2020 set income_redist_bin = 10 where income_redist >= 200000;
``` 

Verify that that everything is populated.

```sql
select * from income_user_2020 limit 10;
select * from income_user_2020 where income_redist_bin < 0;
```

If there are any values with `income_redist_bin` still `-1`, this is most likely out of bounds since the income is too high. These can just be set to the highest bin value.

```sql
update income_user_2020 set income_redist_bin = 10 where income_redist_bin < 0;
```

## Step 6: Post-Stratification

Normally we only use `county` as the group for post-stratification. However, in this case we have two groups (`county` and `year_week`) so we need to create another data table to separate out the results. We use the redistributed income predictions from the full user history in this post-stratification process.

### Step 6.1: Create table

Create the new data table. This is very similar to the previous data table but also holds the entry for the `year_week`.

```sql
create table income_yw_user_2020 (
    user_id varchar(30),
    year_week varchar(10),
    user varchar(20),
    cnty int(5),
    income_pred double,
    income double,
    income_bin double,
    income_redist double,
    income_redist_bin double
);
```

### Step 6.2: Insert data

Insert the data from the feature tables into the data table using the following command. Any value of `-1` is empty for now and will be filled eventually.

```sql
insert into income_yw_user_2020 select group_id, substring(group_id, 1, 7), substring(group_id, 9), -1, group_norm, exp(group_norm), -1, -1, -1 from feat$dd_incomePLOS$timelines2020$yw_user_id$1gra;
```

Delete any entries that have an invalid `year_week` value.

```sql
delete from income_yw_user_2020 where year_week like '%\:%';
```

Check the data table to ensure things look correct. Most columns will be `-1` which we will populate in the next steps.

```sql
select * from income_yw_user_2020 limit 10;
```

### Step 6.3: Update fields

Populate the fields of the `year-week + user_id` data table using the `user_id` data table.

```sql
update income_yw_user_2020 t1, income_user_2020 t2 
    set t1.cnty = t2.cnty, 
        t1.income_pred = t2.income_pred, 
        t1.income = t2.income, 
        t1.income_bin = t2.income_bin, 
        t1.income_redist = t2.income_redist, 
        t1.income_redist_bin = t2.income_redist_bin 
    where t1.user = t2.user_id;
```

Delete any missing users that don't have a valid county.

```sql
delete from income_yw_user_2020 where cnty = -1;
```

Verify that that everything is populated.

```sql
select * from income_yw_user_2020 limit 10;
```

### Step 6.4: Python script to create weights

Run the `create_yw_weights_income.py` script to create the post stratification weights. Use the `year_week + user_id` data table as the user_table. The census_table `cnty_income_2019` is the census data for the 2019 and 2020 years.

```sh
python create_yw_weights_income.py \
    --db ctlb2 \
    --user_table income_yw_user_2020 \
    --census_table cnty_income_2019 \
    --output /path/to/weights.csv
```

The post-stratification weights are stored as a csv without any header. The data is stored in the following format.

```csv
yw_user_id, year_week, county, weight
```
