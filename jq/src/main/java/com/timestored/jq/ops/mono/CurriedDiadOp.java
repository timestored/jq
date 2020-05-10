package com.timestored.jq.ops.mono;

import com.timestored.jq.ops.Diad;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class CurriedDiadOp extends BaseMonad {
	private final Diad diad;
	@Setter @Getter private Object left;
	@Setter @Getter private Object right;
	
	@Override public String name() { 
		return this.diad.name() + "[" + (left == null ? (";" + right) : (left + ";")) + "]"; 
	}

	@Override public String toString() { return name(); }
	
	@Override public Object run(Object a) { return left != null ? diad.run(left, a) : diad.run(a, right); }

}
