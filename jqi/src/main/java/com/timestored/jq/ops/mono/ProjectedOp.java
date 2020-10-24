package com.timestored.jq.ops.mono;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.Context;
import com.timestored.jq.Frame;
import com.timestored.jq.RankException;
import com.timestored.jq.ops.Diad;
import com.timestored.jq.ops.Op;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class ProjectedOp implements Op {
	private final Op op;
    @Setter @Getter protected Frame frame;
    @Setter @Getter protected Context context;
	@Setter @Getter private Object[] args;
	@Getter private int requiredArgumentCount = 0;
	
	public ProjectedOp(Op op, Object[] args) {
		this.op = Preconditions.checkNotNull(op);
		this.args = Preconditions.checkNotNull(args);
		for(int i=0; i<args.length; i++) {
			if(args[i] == null) {
				requiredArgumentCount++;
			}
		}
	}
	
	@Override public String name() {
		// Hide the semi-colon for more typical display
		if(op instanceof Diad && args[0]!=null && Database.QCOMPATIBLE) {
			return this.op.name() + "[" + Qs1Op.B3.asString(args[0]) + "]";
		}
		String s = this.op.name() + "[";
		s += args[0] == null ? "" : Qs1Op.B3.asString(args[0]);
		for(int i=1; i<args.length; i++) {
			s += ";" + (args[i] == null ? "" :  Qs1Op.B3.asString(args[i]));
		}
		return s + "]"; 
	}

	@Override public String toString() { return name(); }
	@Override public short typeNum() { return 104; }

	@Override public Object run(Object[] newArgs) {
		if(newArgs.length > requiredArgumentCount) {
			throw new RankException();
		}
		Object[] argsNow = new Object[args.length];
		int j = 0;
		for(int i=0; i<args.length; i++) {
			argsNow[i] = args[i]!=null ? args[i] : newArgs[j++]; 
		}
		return op.run(argsNow); 
	}


}
