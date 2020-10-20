package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.kexception.NYIException;

public class EnlistOp extends BaseMonad {
	public static EnlistOp INSTANCE = new EnlistOp();
	@Override public String name() { return "enlist"; }
	
	@Override public Object run(Object a) {
		if(a instanceof Col) {
			return MemoryObjectCol.of(a);
		}
		try {
			short ta = TypeOp.TYPE.type(a);
			CType cType = CType.getType(ta < 20 ? ta : 0);
			Col c = ColProvider.getInMemory(cType, 1);
			c.setObject(0, a);
			return c;
		} catch(ClassCastException | NYIException e) {
			return MemoryObjectCol.of(a);
		}
	}
}
