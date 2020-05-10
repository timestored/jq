package com.timestored.jq.ops; 
import com.timestored.jdb.col.*;

public class EqualOp extends NaiveEqualOp {
	public static EqualOp INSTANCE = new EqualOp(); 
	@Override public String name() { return "="; }
	
	/**
	 * In equality operations nulls are treated special as Double.isNan works but == doesn't
	 **/
    	
    @Override public boolean ex(double a, double b) { return  (a == b) | Double.isNaN(a) && Double.isNaN(b); }

    @Override public BooleanCol ex(double a, DoubleCol b) {
    	return ColProvider.b(b.size(), i -> (a == b.get(i)) | Double.isNaN(a) && Double.isNaN(b.get(i)));
    }

    @Override public BooleanCol ex(DoubleCol a, double b) {
    	return ColProvider.b(a.size(), i -> (a.get(i) == b) | Double.isNaN(a.get(i)) && Double.isNaN(b));
    }

    @Override public BooleanCol ex(DoubleCol a, DoubleCol b) {
    	return ColProvider.b(b.size(), i -> (a.get(i) == b.get(i)) | Double.isNaN(a.get(i)) && Double.isNaN(b.get(i)));
    }
    
    @Override public BooleanCol ex(float a, FloatCol b) {
    	return ColProvider.b(b.size(), i -> (a == b.get(i)) | Float.isNaN(a) && Float.isNaN(b.get(i)));
    }

    @Override public BooleanCol ex(FloatCol a, float b) {
    	return ColProvider.b(a.size(), i -> (a.get(i) == b) | Float.isNaN(a.get(i)) && Float.isNaN(b));
    }

    @Override public BooleanCol ex(FloatCol a, FloatCol b) {
    	return ColProvider.b(b.size(), i -> (a.get(i) == b.get(i)) | Float.isNaN(a.get(i)) && Float.isNaN(b.get(i)));
    }

    @Override public boolean ex(float a, float b) { return  (a == b) | Float.isNaN(a) && Float.isNaN(b); }
}