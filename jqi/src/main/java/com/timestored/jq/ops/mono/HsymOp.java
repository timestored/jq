package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.StringCol;
import com.timestored.jq.TypeException;

public class HsymOp extends BaseMonad {
	public static HsymOp INSTANCE = new HsymOp();
	@Override public String name() {return "hsym"; }

	@Override public Object run(Object a) {
		if(CountOp.INSTANCE.count(a)==0) {
			return ColProvider.emptyStringCol;
		} else if(a instanceof String) {
			String s = (String) a;
			return s.startsWith(":") ? s : ":" + s;
		} else if(a instanceof StringCol) {
			return ((StringCol)a).map( s -> (s.startsWith(":") || s.length()==0 ? s : ":"+s));
		}
        throw new TypeException("Couldn't hsym " + a); 
	}

}
