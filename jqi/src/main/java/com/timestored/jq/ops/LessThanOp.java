package com.timestored.jq.ops; 
import com.timestored.jdb.col.*;

public class LessThanOp extends NaiveLessThanOp {
	public static LessThanOp INSTANCE = new LessThanOp(); 
	
    @Override public boolean ex(double a, double b) { return  Double.isNaN(a) ? !Double.isNaN(b) : a < b; }

    @Override public BooleanCol ex(double a, DoubleCol b) {
    	return ColProvider.b(b.size(), i ->  Double.isNaN(a) ? !Double.isNaN(b.get(i)) : a < b.get(i));
    }

    @Override public BooleanCol ex(DoubleCol a, double b) {
    	return ColProvider.b(a.size(), i ->  Double.isNaN(a.get(i)) ? !Double.isNaN(b) : a.get(i) < b);
    }

    @Override public BooleanCol ex(DoubleCol a, DoubleCol b) {
    	return ColProvider.b(b.size(), i ->  Double.isNaN(a.get(i)) ? !Double.isNaN(b.get(i)) : a.get(i) < b.get(i));
    }
    
    
    	
    @Override public boolean ex(float a, float b) { return  Float.isNaN(a) ? !Float.isNaN(b) : a < b; }

    @Override public BooleanCol ex(float a, FloatCol b) {
    	return ColProvider.b(b.size(), i ->  Float.isNaN(a) ? !Float.isNaN(b.get(i)) : a < b.get(i));
    }

    @Override public BooleanCol ex(FloatCol a, float b) {
    	return ColProvider.b(a.size(), i ->  Float.isNaN(a.get(i)) ? !Float.isNaN(b) : a.get(i) < b);
    }

    @Override public BooleanCol ex(FloatCol a, FloatCol b) {
    	return ColProvider.b(b.size(), i ->  Float.isNaN(a.get(i)) ? !Float.isNaN(b.get(i)) : a.get(i) < b.get(i));
    }

    @Override public boolean ex(String a, String b) { return a.compareTo(b) < 0; } 
    @Override public BooleanCol ex(String a, StringCol b) { 
    	return ColProvider.b(b.size(), i ->  a.compareTo(b.get(i)) < 0);
    }
    @Override public BooleanCol ex(StringCol a, String b) { 
    	return ColProvider.b(a.size(), i ->  a.get(i).compareTo(b) < 0);
    }
    @Override public BooleanCol ex(StringCol a, StringCol b) { 
    	return ColProvider.b(b.size(), i ->  a.get(i).compareTo(b.get(i)) < 0);
    }
}