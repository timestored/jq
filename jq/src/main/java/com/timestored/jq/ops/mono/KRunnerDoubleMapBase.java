package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryDoubleCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.IntegerMappedVal;
import com.timestored.jdb.database.LongMappedVal;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.Op;

/**
 * Base class that can be inherited from to implement anything->double uniform mappings.
 * It moves the conversion boiler-plate to one place.
 * Notice it relies on copying all existing values to a double array and then reusing that array.
 */
public abstract class KRunnerDoubleMapBase extends BaseMonad {

	private static final DoubleCol EMPTY = (DoubleCol) ColProvider.emptyCol(CType.DOUBLE);
	
    public double ex(boolean a) { return doCalc(a ? 1.0 : 0.0); }
    public double ex(short a) { return a == SpecialValues.nh ? SpecialValues.nf : doCalc(a); }
    public double ex(int a) { return a == SpecialValues.ni ? SpecialValues.nf : doCalc(a); }
    public double ex(long a) { return a == SpecialValues.nj ? SpecialValues.nf : doCalc(a); }
    public double ex(float a) { return Float.isNaN(a) ? SpecialValues.nf : doCalc(a); }
    public double ex(char a) { return doCalc((int) a); }
    public double ex(double a) { return doCalc(a); }

    double doCalc(double v) {
    	MemoryDoubleCol m =  new MemoryDoubleCol(1);
    	m.set(0, v);
        return placeCalcInto(m).get(0);
    }
    public abstract DoubleCol placeCalcInto(DoubleCol valsAndTarget);

    public DoubleCol ex(BooleanCol a) { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(a)); };
    public DoubleCol ex(ShortCol a)   { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(a)); };
    public DoubleCol ex(IntegerCol a)     { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(a)); };
    public DoubleCol ex(LongCol a)    { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(a)); };
    public DoubleCol ex(FloatCol a)   { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(a)); };
    public DoubleCol ex(CharacterCol a)   { return a.size()==0 ? EMPTY : placeCalcInto(CastOp.CAST.f(CastOp.CAST.i(a))); };
    public DoubleCol ex(DoubleCol a)  { return a.size()==0 ? EMPTY : placeCalcInto(a); };

    public Object run(Object a) {
        if(a instanceof Boolean) {
            return ex((boolean)a);
        } else if(a instanceof Short) {
            return ex((short)a);
        } else if(a instanceof Integer) {
            return ex((int)a);
        } else if(a instanceof Long) {
            return ex((long)a);
        } else if(a instanceof Float) {
            return ex((float)a);
        } else if(a instanceof Double) {
            return ex((double)a);
        } else if(a instanceof Character) {
            return ex((char)a);
        } else if(a instanceof IntegerMappedVal) {
            return ex(((IntegerMappedVal)a).getInt());
        } else if(a instanceof LongMappedVal) {
            return ex(((LongMappedVal)a).getLong());
        } else if(a instanceof BooleanCol) {
            return ex((BooleanCol)a);
        } else if(a instanceof ShortCol) {
            return ex((ShortCol)a);
        } else if(a instanceof IntegerCol) {
            return ex((IntegerCol)a);
        } else if(a instanceof LongCol) {
            return ex((LongCol)a);
        } else if(a instanceof FloatCol) {
            return ex((FloatCol)a);
        } else if(a instanceof DoubleCol) {
            return ex((DoubleCol)a);
        } else if(a instanceof CharacterCol) {
            return ex((CharacterCol)a);
        } else if(a instanceof ObjectCol) {
            return handleNesting((ObjectCol)a);
        }
        throw new UnsupportedOperationException("bad type combo");
    }


    private ObjectCol handleNesting(ObjectCol o) {
    	ObjectCol r = new MemoryObjectCol(o.size());
        for(int i=0; i<r.size(); i++) {
            r.set(i, run(o.get(i)));
        }
        return r;
    }
    
	@Override public String toString() { return Op.toString(this); }
}
