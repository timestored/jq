package com.timestored.jq.ops; 
import com.timestored.jq.ops.mono.logOp;


public class XlogOp extends BaseDiad {
	public static XlogOp INSTANCE = new XlogOp();
	@Override public String name() { return "xlog"; }
	@Override public Object run(Object a, Object b) {
        return DivideOp.INSTANCE.run(logOp.INSTANCE.run(b), logOp.INSTANCE.run(a));
	}
}