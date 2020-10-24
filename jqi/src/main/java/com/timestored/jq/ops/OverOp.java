package com.timestored.jq.ops;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.TypeOp;

public class OverOp extends BaseDiad {
	public static OverOp INSTANCE = new OverOp(); 
	@Override public String name() { return "over"; }

	@Override public Object run(Object a, Object b) {
		long c = CountOp.INSTANCE.count(b);
		if(c == 0) {
			return ColProvider.emptyCol(CType.OBJECT);
		}
		if(TypeOp.isNotList(b) || c <= 1) {
			return b;
		}

		if(a instanceof Diad) {
			Diad d = (Diad) a;
			IndexOp.INSTANCE.setContext(context);
			IndexOp.INSTANCE.setFrame(frame);
			d.setContext(context);
			d.setFrame(frame);
			
			Object prevRes = IndexOp.INSTANCE.run(b, 0);
			for(int i=1; i<c; i++) {
				Object secondArg = IndexOp.INSTANCE.run(b, i);
				prevRes = d.run(prevRes, secondArg);
			}
			return prevRes;	
		}
		throw new TypeException("Only Diads supported for now");
	}
  	
}