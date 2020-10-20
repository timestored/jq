package com.timestored.jq.ops.mono;

import java.util.function.Function;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jq.TypeException;

public class FloorOp extends MonadReduceToObject {

    public static final FloorOp INSTANCE = new FloorOp();
	@Override public String name() { return "floor"; }

	@Override public Object ex(float a)  { return Math.round(Math.floor(a)); }
	@Override public Object ex(double a) { return Math.round(Math.floor(a)); }

	@Override public Object ex(String a) { return a; }
	@Override public Object ex(StringCol a) { return a; }

	@Override public Object ex(boolean a) { return tt(a); }
	@Override public Object ex(BooleanCol a) { return tt(a); }
	@Override public Object ex(short a) { return tt(a); }
	@Override public Object ex(ShortCol a) { return tt(a); }
	
	private static Object tt(Object a) {
    	if(Database.QCOMPATIBLE) {
    		throw new TypeException(); 
    	}
    	return a; 
	}
	
	@Override public LongCol ex(FloatCol a) {
        Function<Integer, Long> f = (i) -> {
        	return Float.isNaN(a.get(i)) || Float.isInfinite(a.get(i)) ?
        			SpecialValues.nj
        			: Math.round(Math.floor(a.get(i)));
        };
		return ColProvider.j(a.size(), f );
    }

	@Override public LongCol ex(DoubleCol a) {
        Function<Integer, Long> f = (i) -> {
        	return Double.isNaN(a.get(i)) || Double.isInfinite(a.get(i)) ?
        			SpecialValues.nj
        			: Math.round(Math.floor(a.get(i)));
        };
		return ColProvider.j(a.size(), f );
    }

}
