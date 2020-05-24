package com.timestored.jq.ops;

import java.util.concurrent.ThreadLocalRandom;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.RandOp;
import com.timestored.jq.ops.mono.TypeOp;

import lombok.Setter;

public class QuestionOp extends BaseDiad {
	public static final QuestionOp INSTANCE = new QuestionOp();
	@Setter private boolean debug = false;
//	private static final Random r = new Random();
	@Override public String name() { return "?"; }
	
	@Override public Object run(Object a, Object b) {
		short ta = TypeOp.TYPE.type(a);
		short tb = TypeOp.TYPE.type(b);
		if(ta <= -4 && ta >= -19) {
			int num = (int) CastOp.CAST.run(CType.INTEGER.getTypeNum(), a);
			CType bCType = CType.getType(tb);
			Col c = ColProvider.getInMemory(bCType, num);
			// Choose randomly from list
			if(b instanceof Col) {
				if(debug) { return 1; }
				for(int i=0; i<num; i++) {
					c.setObject(i, IndexOp.INSTANCE.run(b, RandOp.INSTANCE.ex(CountOp.INSTANCE.count(b))));
				}
			} else if(tb < 0) {
				if(debug) { return 1; }
			// "int ? atom" -> Generate int randoms within bounds of 0-atom
				for(int i=0; i<num; i++) {
					c.setObject(i, RandOp.INSTANCE.run(b));
				}
			}
			return c;
		}

		throw new NYIException("? find not implemented yet");
	}
	
	
}
