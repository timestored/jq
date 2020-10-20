package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jq.TypeException;

public class WhereOp extends BaseMonad {
	public static WhereOp INSTANCE = new WhereOp();
	@Override public String name() { return "where"; }

    @Override public Object run(Object o) {
        if(o instanceof BooleanCol) {
            return ex((BooleanCol) o);  	
        } else if(o instanceof LongCol) {
            return ex((LongCol) o);  	
        } else if(o instanceof IntegerCol) {
            return ex((IntegerCol) o);  	
        } 
        throw new TypeException();
    }

    public LongCol ex(LongCol a) {
    	int sum = 0;
    	boolean containsNonBoolean = false;
    	for(int i=0; i<a.size(); i++) {
    		containsNonBoolean |= (a.get(i) != 0 && a.get(i) != 1);
    		sum += a.get(i);
    	}
    	LongCol r = new MemoryLongCol(sum);
    	if(containsNonBoolean) {
    		// for each item in list, generate that number of values 2 3 0 1~ 0 0 1 1 1 3
    		long v = 0;
    		int p=0;
        	for(int i=0; i<a.size();i++) {
        		for(int j=0; j<a.get(i); j++) {
        			r.set(p++, v);
        		}
        		v++;
        	}
    	} else {
    		// Convert 1/0s to where true
    		int rpos = 0;
        	for(int i=0; i<a.size(); i++) {
        		if(a.get(i) != 0) {
        			r.set(rpos++, i);
        		}
        	}
    	}
    	return r;
    }
    
    public LongCol ex(IntegerCol a) {
    	int sum = 0;
    	boolean containsNonBoolean = false;
    	for(int i=0; i<a.size(); i++) {
    		containsNonBoolean |= (a.get(i) != 0 && a.get(i) != 1);
    		sum += a.get(i);
    	}
    	LongCol r = new MemoryLongCol(sum);
    	if(containsNonBoolean) {
    		// for each item in list, generate that number of values 2 3 0 1~ 0 0 1 1 1 3
    		long v = 0;
    		int p=0;
        	for(int i=0; i<a.size();i++) {
        		for(int j=0; j<a.get(i); j++) {
        			r.set(p++, v);
        		}
        		v++;
        	}
    	} else {
    		// Convert 1/0s to where true
    		int rpos = 0;
        	for(int i=0; i<a.size(); i++) {
        		if(a.get(i) != 0) {
        			r.set(rpos++, i);
        		}
        	}
    	}
    	return r;
    }
    
    public LongCol ex(BooleanCol a) {
    	int sum = 0;
    	for(int i=0; i<a.size(); i++) {
    		sum += a.get(i) ? 1 : 0;
    	}
    	LongCol r = new MemoryLongCol(sum);
		// Convert 1/0s to where true
		int rpos = 0;
    	for(int i=0; i<a.size(); i++) {
    		if(a.get(i)) {
    			r.set(rpos++, i);
    		}
    	}
    	return r;
    }
}
