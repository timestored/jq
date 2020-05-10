package com.timestored.jq.ops;

import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.CurriedDiadOp;

public class DotOp extends BaseDiad {
	public static DotOp INSTANCE = new DotOp(); 
	@Override public String name() { return "."; }

	@Override public Object run(Object a, Object b) {
		if(a instanceof Diad) {
			Diad d = (Diad) a;
			if(2 != CountOp.INSTANCE.count(b)) {
				throw new TypeException("Arg to apply operator . must have two items");
			}
			Object leftArg = IndexOp.INSTANCE.run(b, 0);
			Object rightArg = IndexOp.INSTANCE.run(b, 1);
			return d.run(leftArg, rightArg);
		}
		throw new NYIException();
	}
  	
}