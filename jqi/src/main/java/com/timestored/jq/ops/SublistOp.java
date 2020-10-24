package com.timestored.jq.ops; 
import static com.timestored.jdb.database.SpecialValues.*;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.iterator.RangeLocations;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.TypeOp;


public class SublistOp extends BaseDiad {
	public static SublistOp INSTANCE = new SublistOp();
	@Override public String name() { return "sublist"; }
	
	@Override public Object run(Object a, Object b) {
		Object chopo = CastOp.CAST.run((short) 6, a);
		if(b instanceof Col) {
			Col c = (Col)b;
			if(chopo instanceof Integer) {
	            return ex((int) chopo, c);
	        } else if(chopo instanceof IntegerCol) {
	            return ex((IntegerCol) chopo, c);
	        }
		} else if(chopo instanceof Col && 0==((Col)chopo).size()) {
        	return ColProvider.emptyCol(TypeOp.TYPE.type(b));
        } 
        return b;
	}
	

	public Col ex(int chop, Col target) {
    	if(CType.isNull(chop)) {
    		throw new TypeException("Cannot sublist nulls");
    	}
    	int sz = target.size();
    	if(chop == 0 || sz == 0) {
    		return ColProvider.emptyCol(CType.getType(target.getType()));
    	}
		int lowerBound = 0;
    	int upperBound = (int) Math.min(chop, sz);
    	if(chop < 0) {
    		upperBound = sz;
    		lowerBound = (int) Math.max(sz - -chop,0);
    	}
    	if(lowerBound == 0 && upperBound==sz) {
    		return target;
    	}
		return target.select(new RangeLocations(lowerBound, upperBound));
    }
    
	  public Col ex(IntegerCol chop,  Col target) {
	    	int startPos = CastOp.CAST.i(IndexOp.INSTANCE.ex(chop, 0));
	    	int count = CastOp.CAST.i(IndexOp.INSTANCE.ex(chop, 1));
	    	if(CType.isNull(startPos) || CType.isNull(count) || count <=0 || startPos>=target.size()) {
	    		return ColProvider.emptyCol(CType.getType(target.getType()));
	    	}
	    	int upperBound = (int) Math.min(startPos + count, target.size());
	        return target.select(new RangeLocations(startPos, upperBound));
	    }
}