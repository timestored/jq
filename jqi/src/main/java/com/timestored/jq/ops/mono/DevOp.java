package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.SpecialValues;

public class DevOp extends MonadReduceToDouble {
	public static DevOp INSTANCE = new DevOp(); 
	@Override public String name() { return "dev"; }

	public double ex(byte a)     { return SpecialValues.nf; }
    public double ex(char a)     { return SpecialValues.nf; }
    public double ex(boolean a)  { return SpecialValues.nf; }
    public double ex(short a)    { return SpecialValues.nf; }
    public double ex(int a)      { return SpecialValues.nf; }
    public double ex(long a)     { return SpecialValues.nf; }
    public double ex(float a)    { return SpecialValues.nf; }
    public double ex(double a)   { return SpecialValues.nf; }

    public double ex(ByteCol a)    { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(BooleanCol a) { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(ShortCol a)   { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(IntegerCol a) { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(LongCol a)    { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(FloatCol a)   { return Math.sqrt(new VarOp().ex(a)); }
    public double ex(DoubleCol a)  { return Math.sqrt(new VarOp().ex(a)); }
	public double ex(CharacterCol a) {return Math.sqrt(new VarOp().ex(a)); }

}
