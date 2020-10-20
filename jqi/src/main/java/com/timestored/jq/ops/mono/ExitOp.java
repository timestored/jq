package com.timestored.jq.ops.mono;

import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class ExitOp extends BaseMonad {
	public static ExitOp INSTANCE = new ExitOp();
	@Override public String name() { return "exit"; }
	
	@Override public Object run(Object a) {
		try {
			int v = (int) CastOp.CAST.run((short) 6, a);
			System.exit(v);
		} catch(ClassCastException e) {
			throw new TypeException();
		}
		return null;
	}

}
