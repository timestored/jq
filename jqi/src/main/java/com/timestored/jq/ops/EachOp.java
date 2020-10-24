package com.timestored.jq.ops;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.TypeOp;

public class EachOp extends BaseDiad {
	public static EachOp INSTANCE = new EachOp(); 
	@Override public String name() { return "each"; }

	@Override public Object run(Object a, Object b) {
		long c = CountOp.INSTANCE.count(b);
		if(c == 0) {
			return ColProvider.emptyCol(CType.OBJECT);
		} else if(b instanceof Mapp) {
			Mapp m = (Mapp) b;
			return new MyMapp(m.getKey(), (Col) run(a,m.getValue()));
		}
		ObjectCol r = new MemoryObjectCol((int) c);
		IndexOp.INSTANCE.setContext(context);
		IndexOp.INSTANCE.setFrame(frame);
		
		if(TypeOp.isNotList(b)) {
			return IndexOp.INSTANCE.run(a, b);
		}
		for(int i=0; i<r.size(); i++) {
			Object target = IndexOp.INSTANCE.run(b, i);
			Object res = IndexOp.INSTANCE.run(a, target);
			r.set(i, res);
		}
		return CastOp.flattenGenericIfSameType(r);
	}
  	
}