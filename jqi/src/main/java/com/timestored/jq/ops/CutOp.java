package com.timestored.jq.ops;

import java.util.Optional;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.ops.mono.EnlistOp;

public class CutOp extends BaseDiad {
	public static CutOp INSTANCE = new CutOp(); 
	@Override public String name() { return "cut"; }

	@Override public Object run(Object a, Object b) {
		Optional<Integer> n = CastOp.CAST.intUnsafe(a);
				
		// Remove first/last n entries
		if(n.isPresent() && b instanceof Col) {
			int chop = n.get();
			if(chop <=0) {
				throw new DomainException("can't cut into negatives");
			}
			if(b instanceof Col) {
				Col c = (Col)b;
				if(c.size() == 0) {
					return ColProvider.emptyCol(CType.OBJECT);
				} else if(chop >= c.size()) {
					return EnlistOp.INSTANCE.run(c);
				}
				IntegerCol ic = ColProvider.i((1 + c.size())/chop, i -> chop*i);
				return UnderscoreOp.INSTANCE.ex(ic, (Col) b);
			}
		}
		return UnderscoreOp.INSTANCE.run(a, b);
	}
}