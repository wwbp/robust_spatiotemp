package org.wwbp.languageFilter;

import au.com.bytecode.opencsv.* ;
import org.apache.commons.cli.* ;
import org.apache.commons.cli.Options ;
//import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;

import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import com.carrotsearch.labs.langid.LangIdV3;
import com.carrotsearch.labs.langid.Model;
import com.carrotsearch.labs.langid.DetectedLanguage;
//import cmu.arktweetnlp.Twokenize;

import java.io.*;
import java.util.*;
import java.util.regex.*;

//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;

public class LanguageFilter
    extends Configured
    implements Tool
{
    public static class LangFilterMapper
	extends Mapper< LongWritable, Text, NullWritable, Text> {

	int messageIndex;
	HashSet<String> languages;
	LangIdV3 langID;

	public void map(LongWritable key, Text tweetLine, Context context)
	    throws IOException, InterruptedException {
	    String tweet = tweetLine.toString();
	    CSVParser csvparser = new CSVParser();
	    String tweet_array[], message, lang, conf;
	    
	    try{
	    	tweet_array = csvparser.parseLine(tweet);
	    } catch (IOException e) {
	    	return;
	    }
	    
	    try{
	    	message = tweet_array[this.messageIndex];
	    } catch (ArrayIndexOutOfBoundsException f) {
	    	return;
	    }
	    try {
			message = StringEscapeUtils.unescapeHtml4(message.trim());
	    } catch (IllegalArgumentException g) {
	    	return;
	    }
	    
	    DetectedLanguage detLang;
	    
	    if (message.equals("") || message.equals("NULL"))
		return;
	    if (message.equals("message")){
		lang = "Language";
		conf = "Confidence";
	    } else {
		// Lang ID here
		detLang = langID.classify(message, true);
		if (this.languages.contains(detLang.langCode)){
		    lang = detLang.langCode;
		    conf = String.format("%.8f",detLang.confidence);
		} else {
		    return;
		}
	    }
	    
	    StringWriter sw = new StringWriter();
	    CSVWriter newTweetWriter = new CSVWriter(sw, ',');
	    int newLength = tweet_array.length + 2;
	    String newTweet[] = new String[newLength];
	    for (int i = 0; i<tweet_array.length; i++){
		if (tweet_array[i].equals("NULL"))
		    tweet_array[i] = "";
		newTweet[i] = tweet_array[i];
	    }
	    newTweet[newLength - 2] = lang;
	    newTweet[newLength - 1] = conf;

	    newTweetWriter.writeNext(newTweet);
	    String newTweetString = sw.toString();
	    newTweetString = newTweetString.substring(0, newTweetString.length() - 1);
	    context.write(NullWritable.get(), new Text(newTweetString));
	}
	
	public void setup(Context context)
	    throws IOException, InterruptedException{
	    // Runs once at the beginning of every task
	    this.messageIndex = context.getConfiguration().getInt("messageIndex", 4);
	    String langs[] = context.getConfiguration().get("languages").split(",");
	    this.languages = new HashSet(Arrays.asList(langs));
	    this.langID = new LangIdV3();
	    //String langs[] = {"en"};
	    //this.langID = new LangIdV3(Model.detectOnly(new HashSet<String>(Arrays.asList(langs))));
	}
    }
    
    public static void main(String args[]){
	try {
	    // Argument Parser
	    Options options = new Options();
	    options.addOption("libjars" , true , "Comma-separated jar file dependencies.") ;
	    options.addOption("input", true, "Location of csv input file(s) [HDFS]");
	    options.addOption("output", true, "Path of output directory to be created");
	    options.addOption("message_field", true,
			      "0-based index of the message field in tweets csv");
	    options.addOption("languages", true, "Comma-separated list of language codes to look for");
	    options.addOption("testing", false, "see if we're testing something");
	    
	    Parser parser = new PosixParser() ;
	    CommandLine cmdline = parser.parse( options , args ) ;
	    if(!cmdline.hasOption( "libjars" ) ||
	       !cmdline.hasOption( "input" ) ||
	       !cmdline.hasOption( "output" ) ||
	       !cmdline.hasOption( "message_field" ) ||
	       !cmdline.hasOption("languages")){
		System.out.println( "Not enough arguments.");
		return;
	    } else {
		System.out.println("Arguments parsed correctly! Moving on to the next step");
	    }
	    // Setting up config to send to mapper/reducers

	    Configuration conf = new Configuration();

	    conf.set("input", cmdline.getOptionValue("input" ));
	    conf.set("output", cmdline.getOptionValue("output"));
	    conf.set("languages", cmdline.getOptionValue("languages")); 
	    conf.setInt("messageIndex", Integer.parseInt(cmdline.getOptionValue("message_field")));

	    //Testing
	    if (cmdline.hasOption("testing")){
		test(Integer.parseInt(cmdline.getOptionValue("message_field")));
	    } else {
		//Submitting Hadoop Job
		int exitCode = ToolRunner.run(conf, new LanguageFilter(), args);
		System.exit(exitCode);
	    }
	} catch (org.apache.commons.cli.ParseException w) {
	    System.out.println("Oops, command line parsing went wrong!"+w);
	} catch (Exception e){
	    System.out.println("Something else went wrong...");
	    System.out.println(e);
	}
    }
    @Override
    public int run(String[] args) throws Exception {
	// Setting up configuration
	Configuration conf = new Configuration(getConf());
        String input = conf.get("input");
        String output = conf.get("output");
	String langs = conf.get("languages");
	String messageTable = new String(input).replaceAll(".*/(.+\\.+\\w+)", "$1");
        Job job = new Job(conf,"Tweet language filtering: ["+messageTable+"] for "+langs);

        //job.setJarByClass(getClass());
        job.setJar("LanguageFilter.jar");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LangFilterMapper.class);
        //job.setReducerClass(Reduce.class);
        //job.setCombinerClass(Reduce.class);                                                                                                                                                                        

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(CountTableWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        System.out.println("[Maarten]\tStarting Job");
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void test(int locationField) throws Exception {
	System.out.println("Testing stuff");
	CSVParser csvparser = new CSVParser();
	LangFilterMapper mapper = new LangFilterMapper();
	mapper.setup(null);
	
	String locations[] = {
	    "Venice, CA",
	    "OMG chek dis out",
	    "I'm here yo",
	    "Guten Tag "
      	};
	long lStartTime, lEndTime;
	double avgTime = 0.0;
	LangIdV3 langID = new LangIdV3();
	DetectedLanguage detLang;

	for (String loc: locations){
	    lStartTime = System.currentTimeMillis();
	
	    System.out.println("--------------------\nLocation: '"+
			       StringEscapeUtils.unescapeHtml4(loc.trim())+"'");

	    detLang = langID.classify(loc, true);
	    System.out.printf("%s - %s \n", detLang.langCode, String.format("%.8f",detLang.confidence));
	    lEndTime = System.currentTimeMillis();
	    System.out.println("Elapsed seconds: " + 1.0*(lEndTime-lStartTime)/1000);
	    avgTime += 1.0*(lEndTime-lStartTime)/(1000*locations.length);
	}

	System.out.println("Average time per location: "+avgTime+" s");
	System.out.println("Done for today :)");
    }
}