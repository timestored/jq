package com.timestored.jq.ops; 
import com.timestored.jdb.col.*;

public class EqualOp extends NaiveEqualOp {
	public static EqualOp INSTANCE = new EqualOp(); 
	@Override public String name() { return "="; }
	
	@Override
	public Object run(Object a, Object b) {
		// Only equals handles strings out of the math operators
		if(a instanceof String && b instanceof String) {
	        return ((String)a).equals((String)b);
	    } else if(a instanceof StringCol && b instanceof String) {
	    	StringCol sca = (StringCol) a;
	    	String sb = (String) b;
	    	return ColProvider.b(sca.size(), i -> sca.get(i).equals(sb));
	    } else if(a instanceof String && b instanceof StringCol) {
	    	String sa = (String) a;
	    	StringCol scb = (StringCol) b;
	    	return ColProvider.b(scb.size(), i -> scb.get(i).equals(sa));
	    }  else if(a instanceof StringCol && b instanceof StringCol) {
	    	StringCol sca = (StringCol) a;
	    	StringCol scb = (StringCol) b;
	    	return ColProvider.b(scb.size(), i -> scb.get(i).equals(sca.get(i)));
	    } else if(a instanceof Op || b instanceof Op) {
	    	return a == b;
	    }
		
		return super.run(a, b);
	}
	
	/**
	 * In equality operations nulls are treated special as Double.isNan works but == doesn't
	 **/
    	
    @Override public boolean ex(double a, double b) { return  (a == b) || Double.isNaN(a) && Double.isNaN(b); }

    @Override public BooleanCol ex(double a, DoubleCol b) {
    	return ColProvider.b(b.size(), i -> (a == b.get(i)) || Double.isNaN(a) && Double.isNaN(b.get(i)));
    }

    @Override public BooleanCol ex(DoubleCol a, double b) {
    	return ColProvider.b(a.size(), i -> (a.get(i) == b) || Double.isNaN(a.get(i)) && Double.isNaN(b));
    }

    @Override public BooleanCol ex(DoubleCol a, DoubleCol b) {
    	return ColProvider.b(b.size(), i -> (a.get(i) == b.get(i)) || Double.isNaN(a.get(i)) && Double.isNaN(b.get(i)));
    }
    
    @Override public BooleanCol ex(float a, FloatCol b) {
    	return ColProvider.b(b.size(), i -> (a == b.get(i)) || Float.isNaN(a) && Float.isNaN(b.get(i)));
    }

    @Override public BooleanCol ex(FloatCol a, float b) {
    	return ColProvider.b(a.size(), i -> (a.get(i) == b) || Float.isNaN(a.get(i)) && Float.isNaN(b));
    }

    @Override public BooleanCol ex(FloatCol a, FloatCol b) {
    	return ColProvider.b(b.size(), i -> (a.get(i) == b.get(i)) || Float.isNaN(a.get(i)) && Float.isNaN(b.get(i)));
    }

    @Override public boolean ex(float a, float b) { return  (a == b) || Float.isNaN(a) && Float.isNaN(b); }
}