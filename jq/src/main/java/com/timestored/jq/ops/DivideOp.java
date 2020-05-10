package com.timestored.jq.ops; 
import static com.timestored.jdb.database.SpecialValues.*;

import com.timestored.jdb.col.*;

import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.TypeException;


public class DivideOp extends BaseDiad {
	public static DivideOp INSTANCE = new DivideOp();
	@Override public String name() { return "%"; }
	
	@Override public Object run(Object a, Object b) {

		// Objects about to get complicated
        if(a instanceof ObjectCol) {
            ObjectCol aa = (ObjectCol) a;
            if(b instanceof ObjectCol) {
                ObjectCol ba = (ObjectCol) b;
                return ex(aa, ba);
            }
            return DivOp.INSTANCE.run(a, b);
        } else if(b instanceof ObjectCol) {
            return DivOp.INSTANCE.run(a, b);
        }
        
		Object da = CastOp.CAST.run((short) 9, a);
		Object db = CastOp.CAST.run((short) 9, b);

		if(da instanceof DoubleCol && db instanceof DoubleCol) {
	        return ex((DoubleCol)da, (DoubleCol)db);
	    }  else if(da instanceof DoubleCol && db instanceof Double) {
	        return ex((DoubleCol)da, (double)db);
	    }  else if(da instanceof Double && db instanceof DoubleCol) {
	        return ex((double)da, (DoubleCol)db);
	    }  else if(da instanceof Double && db instanceof Double) {
	        return ex((double)da, (double)db);
	    } 
        throw new TypeException();
	}

	/**
	 * Copy pasted from an early generation of DivOp.
	 */
    public double ex(double c, double d) { 
    	return  (c == nf ? nf : d == nf ? nf :  (c / d)); }

    public DoubleCol ex(double a, DoubleCol b) {
        return a == nf ? b.map(nf) : b.map(c -> (c == nf ? c :  (a / c)));
    }

    public DoubleCol ex(DoubleCol a, double b) {
        return b == nf ? a.map(nf) : a.map(c -> (c == nf ? c :  (c / b)));
    }

    public DoubleCol ex(DoubleCol a, DoubleCol b) {
    	return a.map(b, (c,d) -> c == nf ? nf : d == nf ? nf :  (c / d));
    }
}