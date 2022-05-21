package org.wwbp ;

import java.io.*;
import org.apache.hadoop.io.*;

public class FeatureTableWritable
       extends MapWritable {
       /**   
	     Class encapsulating a feature table for one group_id.
	     This is meant to be a HashMap of Text, TupleWritable.
	     Note: not the same as the old school WWBP feature tables
	           which contain features for multiple group_ids.
		   
       	     Here's a Python like representation of the table:
	     {
		"feat1": (35, .00065)
	     }
	*/   
    
    public FeatureTableWritable(){
	super();
    }
    public FeatureTableWritable(MapWritable otherMap){
	super(otherMap);
    }
    
    public String toString(){
	String ret = "{ ";
	for (Writable k: this.keySet()){
	    ret += "'"+k+"': "+this.get(k)+", ";
	}
	return ret+"}";
    }

    public Writable addToken(Text token){
	IntDoubleWritable previousTuple = (IntDoubleWritable) this.remove(token);
	this.put(token,
		 previousTuple == null ?
		 new IntDoubleWritable(1,0):
		 new IntDoubleWritable(previousTuple.getFirstValue()+1,0));
	return previousTuple;
    }

    public Writable addToken(String token){
	return this.addToken(new Text(token));
    } 
    
    public Writable putFeat(Text feat, IntWritable count){
	/**
	   Given a feature and a count, updates the map accordingly
	   @param feat  the feature to insert/update
	   @param count  the count associated with the feature
	   @return the value that was in the map for "feat" before update,
	           null is there was none
	 */
	IntDoubleWritable previousTuple = (IntDoubleWritable) this.remove(feat);
	this.put(feat,
		 previousTuple == null ?
		 new IntDoubleWritable(count,0) :
		 new IntDoubleWritable(previousTuple.getFirstValue()+count.get(), 0)
		 );
	return previousTuple;
    }
    public Writable putFeat(String feat, int count){
	return this.putFeat(new Text(feat), new IntWritable(count));
    }

    public void mergeFeatureTables(FeatureTableWritable otherTable){
	for (Writable tok: otherTable.keySet()){
	    Text t = (Text) tok;
	    IntDoubleWritable tup = (IntDoubleWritable) otherTable.get(tok);
	    this.putFeat(t.toString(), tup.getFirstValue());
	}
    }

    public void updateGroupNorms(){
	long totalSum = 0;
	for (Writable tok: this.keySet()){
	    Text t = (Text) tok;
	    IntDoubleWritable tuple = (IntDoubleWritable) this.get(t);
	    totalSum += tuple.getFirstValue();
	}
	for (Writable tok: this.keySet()){
	    Text t = (Text) tok;
	    IntDoubleWritable tuple = (IntDoubleWritable) this.remove(t);
	    
	}
    }

    public static class IntDoubleWritable extends TupleWritable<IntWritable, DoubleWritable>{
	public IntDoubleWritable(int x, double y){
	    this.x = new IntWritable(x);
	    this.y = new DoubleWritable(y);
	}
	public IntDoubleWritable(IntWritable x, DoubleWritable y){
	    this.x = new IntWritable(x.get());
	    this.y = new DoubleWritable(y.get());
	}
	public IntDoubleWritable(IntWritable x, double y){
	    this.x = new IntWritable(x.get());
	    this.y = new DoubleWritable(y);
	}
	public IntDoubleWritable(int x, DoubleWritable y){
	    this.x = new IntWritable(x);
	    this.y = new DoubleWritable(y.get());
	}
	public int getFirstValue(){
	    return ((IntWritable) this.x).get();
	}
	public double getSecondValue(){
	    return ((DoubleWritable) this.y).get();
	}
    }
}