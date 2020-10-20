package com.timestored.jq.ops;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.TypeOp;

public class PriorOp extends BaseDiad {
	public static PriorOp INSTANCE = new PriorOp(); 
	@Override public String name() { return "prior"; }

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
			ObjectCol r = new MemoryObjectCol((int) c);
			IndexOp.INSTANCE.setContext(context);
			IndexOp.INSTANCE.setFrame(frame);
			d.setContext(context);
			d.setFrame(frame);
			
			Object prevArg = IndexOp.INSTANCE.run(b, 0);
			if(a instanceof DivideOp) {
				prevArg = CastOp.CAST.run((short) 9, prevArg);
			}
			r.set(0, prevArg);
			for(int i=1; i<r.size(); i++) {
				Object secondArg = IndexOp.INSTANCE.run(b, i);
				Object res = d.run(secondArg, prevArg);
				r.set(i, res);
				prevArg = secondArg;
			}
			return CastOp.flattenGenericIfSameType(r);	
		}
		throw new TypeException("Only Diads supported for now");
	}
  	
}