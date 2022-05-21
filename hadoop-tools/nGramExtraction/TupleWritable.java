package org.wwbp ;

import java.io.*;
import org.apache.hadoop.io.*;

public class TupleWritable<X extends Writable, Y extends Writable> implements Writable {
    public X x;
    public Y y;
    
    public TupleWritable(){
	this.x = null;
	this.y = null;
    }
    
    public String toString(){
	String ret = "(";
	ret += this.x + ", " ;
	ret += this.y + ")";
	return ret;
    }
    
    public TupleWritable(X x, Y y){
	this.x = x;
	this.y = y;
    }

    public void write(DataOutput out) throws IOException {
	this.x.write(out);
	this.y.write(out);
    }
       
    public void readFields(DataInput in) throws IOException {
	this.x.readFields(in);
	this.y.readFields(in);
    }
       
    public static TupleWritable read(DataInput in) throws IOException {
	TupleWritable w = new TupleWritable();
	w.readFields(in);
	return w;
    }
    
    public X getFirst(){
	return (X) this.x;
    }
    public Y getSecond(){
	return (Y) this.y;
    }
}