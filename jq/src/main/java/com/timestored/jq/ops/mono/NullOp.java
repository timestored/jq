package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.ops.EqualOp;

public class NullOp extends BaseMonad {
	public static NullOp INSTANCE = new NullOp();
	@Override public String name() { return "null"; }

    public Object run(Object o) {
    	// boolean list doesn't have null values
    	if(o instanceof BooleanCol) {
    		return new MemoryBooleanCol(((BooleanCol)o).size());
    	} else if(o instanceof Boolean) {
    		return false;
    	}
    	Object nullVal = CType.getType((short) -Math.abs(TypeOp.TYPE.type(o))).getNullValue();
    	return EqualOp.INSTANCE.run(o, nullVal);
    }
}
