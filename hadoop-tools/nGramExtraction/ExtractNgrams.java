package org.wwbp ;

import java.io.IOException;
import java.io.StringWriter;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.*;
import cmu.arktweetnlp.Twokenize;
import java.util.*;
import java.util.regex.*;
import java.text.*;
import java.io.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import au.com.bytecode.opencsv.* ;
//import org.json.simple.JSONValue;
import org.apache.commons.cli.* ;
import org.apache.commons.cli.Options ;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat ;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat ;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat ;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class ExtractNgrams
extends Configured
implements Tool
{
  public static void main( String args[])
  {
    try {
      // Argument parser
      Options options = new Options() ;
      options.addOption( "libjars" , true , "Comma-separated jar file dependencies." ) ;
      options.addOption("files" , true , "Comma-separated files dependencies." ) ;
      options.addOption("input" , true , "Input csv files in HDFS. Can be used multiple times to specify more than one file." ) ;
      options.addOption("output" , true , "Output directory in HDFS. Make sure it doesn't exist yet, hadoop creates it and can't overwrite." ) ;
      options.addOption("message_field" , true , "Index of the message field. Zero indexed." ) ;
      options.addOption("group_id_index" , true , "Index of group id to use." ) ;
      options.addOption("n" , true , "N in n grams to be extracted. 1grams, 2grams, ..." ) ;
      options.addOption("byGroupByDate", true, "group by group_id and by date. Argument is regex for the date");
      options.addOption("dateIndex", true, "dateIndex");
      options.addOption("testing" , false , "If testing, hadoop job isn't started" ) ;

      Parser parser = new PosixParser() ;
      CommandLine cmdline = parser.parse( options , args ) ;
      if( !cmdline.hasOption( "libjars" ) ||
      !cmdline.hasOption( "input" ) ||
      !cmdline.hasOption( "output" ) ||
      !cmdline.hasOption( "message_field" ) ||
      !cmdline.hasOption( "group_id_index" ) ){
        System.out.println( "Not enough arguments." );
      } else {
        if (cmdline.hasOption("byGroupByDate") && !cmdline.hasOption("dateIndex")){
          System.out.println( "Please indicate the index of the datetime" );
        }
        System.out.println("Command line parsed correctly: Extracting "
        +cmdline.getOptionValue("n")+"-gram features from HDFS:"+cmdline.getOptionValue("input"));

      }
      Configuration conf = new Configuration() ;
      conf.set( "input" , cmdline.getOptionValue( "input" ) ) ;
      if (cmdline.hasOption( "files" ) &&
      cmdline.getOptionValue("files").trim().length() > 0){
        System.out.println("Using the one grams in "+cmdline.getOptionValue("files")+" as whitelist");
        String fn = cmdline.getOptionValue("files");
        fn = fn.substring(fn.lastIndexOf("/")+1);
        System.out.println("Filename sent to nodes: "+fn+" [CTRL-C if this seems wrong!!!]");
        conf.set("whitelistFile", fn);
      } else {
        System.out.println("Not using whitelist!");
      }
      conf.set( "output" , cmdline.getOptionValue( "output" ) ) ;
      conf.setInt( "ngramsN" , Integer.parseInt(cmdline.getOptionValue("n")));
      conf.setInt( "msgsIndex", Integer.parseInt(cmdline.getOptionValue("message_field")));
      conf.setInt( "grpIdIndex" , Integer.parseInt(cmdline.getOptionValue("group_id_index")));

      //TODO: If group_id is a day or a month, set to true
      if (cmdline.hasOption("byGroupByDate")){
        conf.setBoolean("twitterDate", false );
        conf.set("byGroupByDate", cmdline.getOptionValue("byGroupByDate"));
        conf.setInt("dateIndex", Integer.parseInt(cmdline.getOptionValue("dateIndex")));
      }

      // Context contains: input, output, ngramsN, msgsIndex, grpIdIndex
      if (cmdline.hasOption("testing")){
        test(cmdline.getOptionValue("byGroupByDate"));
        System.out.println("NEED TO CHECK byGroupByDate");
      }else{
        int exitCode = ToolRunner.run(conf, new ExtractNgrams(), args);
        System.exit(exitCode);
      }
    } catch (org.apache.commons.cli.ParseException w) {
      System.out.println("Oops, command line parsing went wrong!");
    } catch (Exception e){
      System.out.println("Something else went wrong..."+e);
      e.printStackTrace();
    }

  }
  public static void test(String dateRegex) throws Exception{
    String tweet1 = "123"+'\t'+"\"432660600947081216\",\"432660600947081216\",\"NULL\",\"NULL\",\"RT @HHSGov: RT, to wish #TeamUSA good \"\"luck\"\" at the #Sochi2014 games! http://t.co/IUPsqJdeVe\",\"2014-02-09 23:41:40\",\"NULL\",\"NULL\",\"NULL\",\"1587036638\",\"NULL\",\"NULL\",\"431993085409497089\",\"California\",\"355\",\"122\",\"\",\"en\",\"1\"";
    String tweet2 = "\"432670006002728960\",\"432660600947081216\",\"NULL\",\"NULL\",\"RT @RedScareBot: Le(a)nin left RT @palmbeachtim: @HHSGov mom would be happier if you could pay for it on your own and not need a subsidy.  &#8230;\",\"2014-02-10 00:19:02\",\"NULL\",\"NULL\",\"NULL\",\"532275984\",\"NULL\",\"NULL\",\"432277266232791041\",\"A-STATE\",\"1574\",\"462\",\"Central Time (US & Canada)\",\"en\",\"NULL\"";

    Text value = new Text(tweet1);

    System.out.println(tweet1);
    System.out.println(tweet1.replaceAll("^\\d+\\t+", ""));


    Map m = new Map();
    m.setup(null);



    CSVParser csvparser = new CSVParser();
    String[] cells;
    cells = csvparser.parseLine(tweet1);

    List<String> toks1 = m.ngramTokenizationWithWhitelist(2,cells[4]);
    FeatureTableWritable ft1 = new FeatureTableWritable();
    for (String tok: toks1){
      ft1.addToken(tok);
    }
    System.out.println(ft1+"\n");

    List<String> toks2 = m.ngramTokenization(2,cells[4]);
    FeatureTableWritable ft2 = new FeatureTableWritable();
    for (String tok: toks2){
      ft2.addToken(tok);
    }
    System.out.println(ft2+"\n");



    /*Pattern date = Pattern.compile(dateRegex);
    System.out.println("PATTERN:"+date.pattern());
    Matcher dateMatcher = date.matcher(cells[5]);

    System.out.println("DATE TIME: ["+cells[5]+"]");
    System.out.println("DATE TIME: ["+cells[5]+"]");
    if (dateMatcher.matches()){
    System.out.println("MATCH:"+dateMatcher.group(1));
  }
  List<String> token1 = Twokenize.tokenize(cells[4]);
  System.out.println(token1);

  List<String> token2 = Twokenize.tokenize(csvparser.parseLine(tweet2)[4]);
  System.out.println(token2);

  String[] a = {"hi", "hi", "there"};
  String[] b = {"hi", "thur"};
  List<String> tokens1 = Arrays.asList("hi", "hi", "there");
  List<String> tokens2 = Arrays.asList("hi", "thur");
  FeatureTableWritable ft1 = new FeatureTableWritable();
  for (String tok: tokens1){
  ft1.addToken(tok);
}
FeatureTableWritable ft2 = new FeatureTableWritable();
for (String tok: tokens2){
ft2.addToken(tok);
}

System.out.println(ft1+"\n");
System.out.println(ft2+"\n");
ft1.mergeFeatureTables(ft2);
System.out.println(ft1+"\n");*/
}
@Override
public int run(String[] args) throws Exception{
  // TODO: add actual argument parsing that works
  // for (String a:args){System.out.println("[Maarten!]\tArg: "+a);}

  // Setting up configuration
  Configuration conf = new Configuration(getConf());
  //conf.set("mapreduce.output.textoutputformat.separator", "");
  //conf.set("mapred.textoutputformat.separator", ""); DEPRECATED?
  int ngramsN = conf.getInt("ngramsN", 1);
  String input = conf.get("input");
  String output = conf.get("output");
  String messageTable = new String(input).replaceAll(".*/(.+)\\.+\\w+", "$1");
  String jobName = ngramsN+"-gram extraction ["+messageTable+"]";
  if (conf.get("byGroupByDate") != null){
    jobName += " by date";
  }
  Job job = new Job(conf,jobName);

  //job.setJarByClass(getClass());
  job.setJar("ExtractNgrams.jar");
  FileInputFormat.addInputPaths(job, input);
  FileOutputFormat.setOutputPath(job, new Path(output));

  job.setMapperClass(Map.class);
  job.setReducerClass(Reduce.class);
  //job.setCombinerClass(Reduce.class);

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(CountTableWritable.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(CountTableWritable.class);
  job.setNumReduceTasks(100);
  System.out.println("[Maarten]\tStarting Job");
  return job.waitForCompletion(true) ? 0 : 1;
}
public static class CountTableWritable extends TwoDArrayWritable
{
  public CountTableWritable(){
    super(Text.class);
  }
}
public static class FeatureTable extends HashMap<String, Integer> {

  private boolean weightedLexicon; //ToDo: handle weighted lex in write!

  public FeatureTable(){
    super();
  }
  public FeatureTable(HashMap<String, Integer> otherMap){
    super(otherMap);
  }
  public String write(String group_id){
    return this.write(group_id, false);
  }
  public String writeBoolean(String group_id){
    return this.write(group_id, true);
  }

  public String write(String group_id, boolean feat16to1){

    String ret = "";

    int length = this.size();
    String[] feats = new String[length];
    long[] values = new long[length];
    double[] gns = new double[length];
    long totalValues = 0;
    int iterate = 0;
    // Filling the features in
    for (String k: this.keySet()){
      feats[iterate++] = k;
    }
    // getting the counts (value) for each feature
    // and adding it to the total count
    for (int i = 0; i < length ; i++){
      totalValues += values[i] = this.get(feats[i]).intValue();
    }
    // Computing the group_norms for each feature
    for (int i = 0; i < length ; i++){
      gns[i] = values[i] / (double) totalValues;
    }
    StringWriter sw = new StringWriter();
    for (int i = 0; i < length ; i++){
      CSVWriter cwv = new CSVWriter(sw);
      String[] line = new String[4];
      line[0] = group_id;
      line[1] = feats[i];
      line[2] = ""+values[i];
      line[3] = feat16to1 ? "1" : (""+gns[i]);
      cwv.writeNext(line);
    }
    String out = sw.toString();
    //System.out.println("AAA");
    //System.out.println(out);
    //System.out.println("BBB");
    //System.out.println(group_id);
    if (out.length() - 1 > 0){
      return out.substring(0, out.length() - 1);
    } else {
      return "";
    }
  }

  public FeatureTable addLexTable(HashMap<String, Set<String>> lexicon){
    //New feat table containing lexicon features
    HashMap<String, Integer> newOne = new FeatureTable();

    // For each 1gram, find categories
    for (String oneGram: this.keySet()){
      // Nb of times oneGram has been used
      int count = this.get(oneGram).intValue();

      for (String category: lexicon.keySet()){
        Integer currentCount = newOne.remove(category);
        if(lexicon.get(category).contains(oneGram)){
          //Add category or add
          newOne.put(category,
          currentCount == null ?
          new Integer(count):
          new Integer(count + currentCount.intValue())
          );
        }
      }
    }
    return new FeatureTable(newOne);
  }
}

public static class Reduce
extends Reducer<Text, CountTableWritable, NullWritable, Text>
{
  // Context contains: input, output, ngramsN, msgsIndex, grpIdIndex
  public void reduce(Text group_id, Iterable<CountTableWritable> featureTables, Context context)
  throws IOException, InterruptedException {

    FeatureTable allFeats = new FeatureTable();

    long totalCount = 0;
    for (CountTableWritable featureTable: featureTables){
      Writable[][] fT = featureTable.get();
      for (int i = 0; i<fT.length;i++){
        String feat = ((Text)fT[i][0]).toString();
        int count = Integer.parseInt(((Text)fT[i][1]).toString());
        totalCount += count;
        Integer currentCount = allFeats.remove(feat);
        allFeats.put(feat,
        currentCount == null ?
        new Integer(count):
        new Integer(count + currentCount.intValue())
        );
      }
    }
    // Tables are merged, now we just write it to the output
    context.write(NullWritable.get(), new Text(allFeats.write(group_id.toString())));
  }
}

public static class Map
extends Mapper<LongWritable, Text, Text, CountTableWritable>
{
  static HashSet<String> oneGramSet;
  public void setup(Context context)
  throws IOException, InterruptedException{
    this.oneGramSet = new HashSet<String>();
    String fn = null;
    if (context == null){
      return;
    }
    fn = context.getConfiguration().get("whitelistFile"); // should return null if there's no file
    if (fn != null){
      BufferedReader in = new BufferedReader(new FileReader(fn));
      String line;
      while ((line = in.readLine()) != null){
        this.oneGramSet.add(line.trim());
      }
    } else {
      this.oneGramSet = null;
    }
  }

  public void map(LongWritable key, Text value, Context context)
  throws IOException, InterruptedException {
    // Setup for grouping and tokenizing
    Configuration conf = context.getConfiguration();
    int n = conf.getInt("ngramsN",1);
    int msgLocIndex = conf.getInt("msgsIndex", 4);
    int groupIdIndex = conf.getInt("grpIdIndex", 1);
    boolean twitterDate = conf.getBoolean("twitterDate", false);
    int dateIndex = conf.getInt("dateIndex", 5);
    String byGroupByDate = conf.get("byGroupByDate");

    String line = value.toString();

    CSVParser csvparser = new CSVParser();
    String[] cells = new String[15];

    try{
      cells = csvparser.parseLine(line);
    } catch (IOException e){
      try{
        cells = csvparser.parseLine(line.replaceAll("^\\d+\\t+", "").replaceAll("\\\\\"", "\\\\ \""));
      } catch (IOException f){
        //throw new IOException("Problem with csv line parsing: "+f+"\n["+line+"] ["+line.replaceAll("^\\d+\\t+", "").replaceAll("\\\\\"", "\\\\ \"")+"]");
        //Arrays.fill(cells, "");
        return;
      }
    }
    String group_id_str;
    String datetime;
    try {
      group_id_str = twitterDate? cells[groupIdIndex].split("\\s+")[0] : cells[groupIdIndex];
      datetime = cells[dateIndex];
    } catch (ArrayIndexOutOfBoundsException e){
      //throw new ArrayIndexOutOfBoundsException("Index: "+groupIdIndex+" - ["+line+"]\n"+e);
      return;
    }
    if (byGroupByDate != null){
      String dateFormat = "yyyy-MM-dd";
      SimpleDateFormat df = new SimpleDateFormat(dateFormat);

      Matcher m = Pattern.compile(byGroupByDate).matcher(datetime);
      if (m.find()){
        // Use date regex to generate year_week
        try{
          Date date = df.parse(m.group(1));
          Calendar cal = Calendar.getInstance();
          cal.setTime(date);
          int week = cal.get(Calendar.WEEK_OF_YEAR);
          int year = cal.get(Calendar.YEAR);
          String year_week = String.valueOf(year) + "_" + String.valueOf(week) ;
          group_id_str = year_week+":"+group_id_str;
        }
        catch (java.text.ParseException p){
          System.out.println("Oops, date can't be parsed!");
        }
      }
      else
      return;
    }
    Text group_id = new Text(group_id_str);

    //header of the csv files exported from MySQL
    try{
      if (cells[msgLocIndex].equals("message")){return;}
    } catch (ArrayIndexOutOfBoundsException p) {
      return;
    }

    List<String> tokens;
    if (this.oneGramSet != null && !this.oneGramSet.isEmpty()){ //throws null pointer exception
      tokens = ngramTokenizationWithWhitelist(n, cells[msgLocIndex]);
    } else {
      tokens = ngramTokenization(n, cells[msgLocIndex]);
      //tokens = ngramTokenizationOnSpacesOnly(n, cells[msgLocIndex]);
      //tokens = ngramTokenizationPOSonly(n, cells[msgLocIndex]);
      //tokens = ngramTokenizationDepsAndBigrams(n, cells[msgLocIndex]);
      //tokens = ngramTokenizationDepsOnly(n, cells[msgLocIndex]);
    }
    if (tokens == null){
      // i.e. the message was shorter than n, so no n-grams available
      return;
    }
    HashMap<String, Integer> tokenCounts = ngramCounts(tokens);
    int nbTokens = tokenCounts.size();
    Text[][] featuresArray = new Text[nbTokens][2];
    int i = 0;
    for (String token: tokenCounts.keySet()){
      // fill the array
      featuresArray[i][0] = new Text(token);
      featuresArray[i++][1] = new Text(tokenCounts.get(token).toString());
    }

    CountTableWritable featureTable = new CountTableWritable();
    featureTable.set(featuresArray);

    context.write(group_id, featureTable);
  }

  public HashMap<String, Integer> ngramCounts(List<String> tokens){
    HashMap<String, Integer> countMap = new HashMap<String, Integer>();
    for (String tok: tokens){
      String token = tok.toLowerCase();
      Integer count = countMap.remove(token);
      countMap.put(token, count == null ? new Integer(1) : new Integer(count.intValue() + 1) );
    }
    return countMap;
  }

  public List<String> ngramTokenization(int n, String text){
    List<String> oneGrams = Twokenize.tokenize(text);

    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          ngram+= oneGrams.get(i+j);
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
  public List<String> ngramTokenizationWithWhitelist(int n, String text){
    List<String> oneGrams = Twokenize.tokenize(text);

    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      String tok;
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          tok = oneGrams.get(i+j);
          if (this.oneGramSet.contains(tok)){
            ngram+= tok;
          } else {
            ngram = "<OOV_"+n+"gram>";
            break;
          }
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
  public List<String> ngramTokenizationOnSpacesOnly(int n, String text){
    /**Don't use this unless you know spaces are actually separating tokens!
    A good example to use this is with the POS tagged messages.
    */
    List<String> oneGrams = Arrays.asList(text.split("\\s+"));

    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          ngram+= oneGrams.get(i+j);
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
  public List<String> ngramTokenizationPOSonly(int n, String text){
    /**Don't use this unless you're working with POS tagged messages.
    */
    String tokens[] = text.split("\\s+");
    for (int i = 0; i<tokens.length;i++){
      tokens[i] = tokens[i].substring(1+tokens[i].lastIndexOf("/"));
    }
    List<String> oneGrams = Arrays.asList(tokens);


    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          ngram+= oneGrams.get(i+j);
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
  public List<String> ngramTokenizationDepsAndBigrams(int n, String text){
    /**Don't use this unless you're working with dependency tagged messages.
    */
    String tokens[] = text.split("\\),");
    for (int i = 0; i<tokens.length;i++){
      //System.out.println("token "+tokens[i]);
      int split=tokens[i].indexOf("(");
      String tok = "";
      tok = tokens[i].substring(1,split);
      tok += " "+tokens[i].substring(split+1,tokens[i].indexOf("-")-1);
      tok += " "+tokens[i].substring(tokens[i].indexOf(",")+2,tokens[i].length()-1);
      tokens[i] = tok;
    }
    List<String> oneGrams = Arrays.asList(tokens);
    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          ngram+= oneGrams.get(i+j);
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
  public List<String> ngramTokenizationDepsOnly(int n, String text){
    /**Don't use this unless you're working with dependency tagged messages.
    */
    String tokens[] = text.split("\\),");
    for (int i = 0; i<tokens.length;i++){
      //System.out.println("token "+tokens[i]);
      int split=tokens[i].indexOf("(");
      String tok = "";
      tok = tokens[i].substring(1,split);
      // tok += " "+tokens[i].substring(split+1,tokens[i].indexOf("-")-1);
      // tok += " "+tokens[i].substring(tokens[i].indexOf(",")+2,tokens[i].length()-1);
      tokens[i] = tok;
    }
    List<String> oneGrams = Arrays.asList(tokens);
    if (n == 1){
      return new ArrayList<String>(oneGrams);
    }
    // check that n isn't bigger than the number of tokens in the message
    if (oneGrams.size() - n + 1 > 0){
      List<String> ngrams = new ArrayList<String>(oneGrams.size()-n+1);
      for (int i=0;i<oneGrams.size()-n+1;i++){
        String ngram = "";
        for (int j=0;j<n;j++){
          ngram+= oneGrams.get(i+j);
          if (j != n-1) {ngram+=' ';}
        }
        ngrams.add(ngram);
        //System.out.println("Token :\t'"+ngram+"'");
      }
      return new ArrayList<String>(ngrams);
    }
    return null;
  }
}
}
