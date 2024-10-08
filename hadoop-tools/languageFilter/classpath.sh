################## CLASSPATH ################
# getting top level of repository i.e. /home/hadoop/hadoopPERMA
repo=`git rev-parse --show-toplevel`
# current directory
export CLASSPATH="."

# apache commons cli
export CLASSPATH="$CLASSPATH:$repo/jars/commons-cli-1.2.jar"

# hadoop
export CLASSPATH="$CLASSPATH:/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/hdfs:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/hdfs/lib/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/hdfs/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/yarn/lib/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/yarn/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/mapreduce/lib/*:/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:/opt/mapr/lib/kvstore*.jar:/opt/mapr/lib/libprotodefs*.jar:/opt/mapr/lib/baseutils*.jar:/opt/mapr/lib/maprutil*.jar:/opt/mapr/lib/json-20080701.jar:/opt/mapr/lib/flexjson-2.1.jar"

# hadoop-lzo-lib
export CLASSPATH="$CLASSPATH:$repo/jars/hadoop-lzo-0.4.21-SNAPSHOT-sources.jar"

# apache commons lang3
export CLASSPATH="$CLASSPATH:$repo/jars/commons-lang3-3.3.2.jar"

# opencsv
export CLASSPATH="$CLASSPATH:$repo/jars/opencsv-2.3.jar"

#LANG ID
export CLASSPATH="$CLASSPATH:$repo/jars/langid-java-1.0.0.jar"

################## HADOOP_CLASSPATH ############
# current directory
export HADOOP_CLASSPATH="."

# apache commons cli
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$repo/jars/commons-cli-1.2.jar"

# hadoop-lzo-lib
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$repo/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar"

# apache commons lang3
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$repo/jars/commons-lang3-3.3.2.jar"

# opencsv
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$repo/jars/opencsv-2.3.jar"

#LANG ID
export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$repo/jars/langid-java-1.0.0.jar"

################## libjars ############ 
libjars="$repo/jars/commons-cli-1.2.jar,$repo/jars/commons-lang3-3.3.2.jar,$repo/jars/opencsv-2.3.jar,$repo/jars/langid-java-1.0.0.jar,$repo/jars/hadoop-lzo-0.4.21-SNAPSHOT.jar"
