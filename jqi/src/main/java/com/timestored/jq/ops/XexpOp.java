package com.timestored.jq.ops;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.TypeOp;

public class XexpOp extends BaseDiad {
	public static XexpOp INSTANCE = new XexpOp(); 
	@Override public String name() { return "xexp"; }

	@Override public Object run(Object a, Object b) {
		if(CountOp.INSTANCE.count(a)==0 || CountOp.INSTANCE.count(b) == 0) {
			short ta = TypeOp.TYPE.type(a);
			short tb = TypeOp.TYPE.type(b);
			if((ta == 0 || tb == 0)) {
				return ColProvider.emptyCol(CType.OBJECT);
			}
			return ColProvider.emptyCol(CType.DOUBLE);			
		}
		
		Object fa = CastOp.CAST.run(CType.DOUBLE.getTypeNum(), a);
		Object fb = CastOp.CAST.run(CType.DOUBLE.getTypeNum(), b);
		if(fa instanceof Double) {
			double dfa = (double) fa;
			if(fb instanceof Double) {
				return ex(dfa, (double)fb);
			} else if(fb instanceof DoubleCol) {
				return ex(dfa, (DoubleCol)fb);
			}
		} else if (fa instanceof DoubleCol) {
			DoubleCol dfa = (DoubleCol) fa;
			if(fb instanceof Double) {
				return ex(dfa, (double)fb);
			} else if(fb instanceof DoubleCol) {
				return ex(dfa, (DoubleCol)fb);
			}
		}
		throw new TypeException();
	}
  	
	public double ex(double a, double b) { return Math.pow(a, b); }
	
	public DoubleCol ex(DoubleCol a, double b) {
		return a.map(c -> Math.pow(c, b));
	}
	
	public DoubleCol ex(double a, DoubleCol b) {
		return b.map(c -> Math.pow(a, c));
	}
	
	public DoubleCol ex(DoubleCol a, DoubleCol b) {
		return a.map(a, (c,d) -> Math.pow(c,d));
	}
}