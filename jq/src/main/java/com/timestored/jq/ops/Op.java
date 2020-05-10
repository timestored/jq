package com.timestored.jq.ops;

import com.timestored.jq.Context;
import com.timestored.jq.Frame;
import com.timestored.jq.TypeException;

public interface Op {
	String name();
	short typeNum();
    default Object ex(Op op) { throw new TypeException(); }
    public void setFrame(Frame frame);
    public void setContext(Context context);

	public static String toString(Op op) {
		return "Op(" + op.name() + " " + op.typeNum() + ")";
	}
}
