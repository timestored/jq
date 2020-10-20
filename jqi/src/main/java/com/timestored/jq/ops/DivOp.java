package com.timestored.jq.ops; 
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.FloorOp;
import com.timestored.jq.ops.mono.TypeOp;

public class DivOp extends NaiveDivOp {
	public static DivOp INSTANCE = new DivOp(); 
	@Override public String name() { return "div"; }
	
	@Override public Object run(Object a, Object b) {
		Object r = super.run(a, b);
		short t =  (short) Math.abs(TypeOp.TYPE.type(r));
		return ((t == 8 || t == 9) && 0 < CountOp.INSTANCE.count(r)) ? CastOp.CAST.run((short) 9, FloorOp.INSTANCE.run(r)) : r;		
	}
}