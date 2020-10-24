package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.ObjectCol;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.GreaterThanOp;
import com.timestored.jq.ops.LessThanOp;
import com.timestored.jq.ops.SubOp;

public class SignumOp extends BaseMonad {
	public static SignumOp INSTANCE = new SignumOp();
	@Override public String name() {return "signum"; }

	@Override public Object run(Object a) {
		if(CountOp.INSTANCE.count(a) == 0 && a instanceof ObjectCol) {
			return a;
		}
		return CastOp.CAST.run((short)6, SubOp.INSTANCE.run(GreaterThanOp.INSTANCE.run(a, 0), LessThanOp.INSTANCE.run(a, 0)));
	}

}
