package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jq.ops.CastOp;

public class ShowOp extends BaseMonad {
	public static ShowOp INSTANCE = new ShowOp();
	@Override public String name() { return "show"; }

    public Object run(Object o) {
    	QsOp.INSTANCE.setContext(context);
    	QsOp.INSTANCE.setFrame(frame);
    	Object r = QsOp.INSTANCE.run(o);
    	context.stdout(CastOp.CAST.s((CharacterCol) r));
    	return NiladicOp.INSTANCE;
    }
}
