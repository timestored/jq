package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.ops.EachOp;
import com.timestored.jq.ops.EqualOp;
import com.timestored.jq.ops.Op;

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
    	short typ = (short) -Math.abs(TypeOp.TYPE.type(o));
    	if(typ == 0) {
    		return CountOp.INSTANCE.count(o) == 0 ? ColProvider.emptyObjectCol : EachOp.INSTANCE.run(NullOp.INSTANCE, o);
    	} 
    	Object nullVal = o instanceof Op ? NiladicOp.INSTANCE : CType.getType(typ).getNullValue();
    	return EqualOp.INSTANCE.run(o, nullVal);
    }
}
