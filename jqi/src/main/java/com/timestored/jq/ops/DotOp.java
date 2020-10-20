package com.timestored.jq.ops;

import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.RankException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.Monad;

public class DotOp extends BaseDiad {
	public static DotOp INSTANCE = new DotOp(); 
	@Override public String name() { return "."; }

	@Override public Object run(Object a, Object b) {
		long c = CountOp.INSTANCE.count(b);
		if(a instanceof Op) {
			Op op = (Op) a;
			if(a instanceof Diad && c==2) {
				Diad d = (Diad) a;
				Object leftArg = IndexOp.INSTANCE.run(b, 0);
				Object rightArg = IndexOp.INSTANCE.run(b, 1);
				return d.run(leftArg, rightArg);
			} else if(a instanceof Monad && c == 1) {
				return ((Monad) a).run(b);
			} else if(c > op.getRequiredArgumentCount()) {
				throw new RankException(op.getRequiredArgumentCount() + " args required but received " + c);
			} 

			Object[] args = new Object[(int) c];
			for(int i=0; i<c; i++) {
				args[i] = IndexOp.INSTANCE.run(b, i);
			}
			return op.run(args);
		}
		throw new NYIException();
	}
  	
}