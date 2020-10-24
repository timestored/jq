package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.database.SpecialValues;

public class SdevOp extends MonadReduceToDouble {
	public static SdevOp INSTANCE = new SdevOp(); 
	@Override public String name() { return "sdev"; }

	@Override public double ex(byte a)     { return SpecialValues.nf; }
    @Override public double ex(char a)     { return SpecialValues.nf; }
    @Override public double ex(boolean a)  { return SpecialValues.nf; }
    @Override public double ex(short a)    { return SpecialValues.nf; }
    @Override public double ex(int a)      { return SpecialValues.nf; }
    @Override public double ex(long a)     { return SpecialValues.nf; }
    @Override public double ex(float a)    { return SpecialValues.nf; }
    @Override public double ex(double a)   { return SpecialValues.nf; }

    public double ex(ByteCol a) { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(BooleanCol a) { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(ShortCol a)   { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(IntegerCol a)     { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(LongCol a)    { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(FloatCol a)   { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(DoubleCol a)  { return Math.sqrt(new SvarOp().ex(a)); }
    public double ex(CharacterCol a)  { return Math.sqrt(new SvarOp().ex(a)); }

}
