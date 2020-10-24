package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jdb.database.SpecialValues;

public class SvarOp extends MonadReduceToDouble {
	public static SvarOp INSTANCE = new SvarOp(); 
	@Override public String name() { return "svar"; }

	@Override public double ex(byte a)     { return SpecialValues.nf; }
    @Override public double ex(char a)     { return SpecialValues.nf; }
    @Override public double ex(boolean a)  { return SpecialValues.nf; }
    @Override public double ex(short a)    { return SpecialValues.nf; }
    @Override public double ex(int a)      { return SpecialValues.nf; }
    @Override public double ex(long a)     { return SpecialValues.nf; }
    @Override public double ex(float a)    { return SpecialValues.nf; }
    @Override public double ex(double a)   { return SpecialValues.nf; }

    @Override public double ex(ByteCol a) 	 { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(BooleanCol a) { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(ShortCol a)   { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(IntegerCol a) { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(LongCol a)    { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(FloatCol a)   { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
    @Override public double ex(DoubleCol a)  { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }
	@Override public double ex(CharacterCol a) { double n = a.size(); return (n/(n-1))*(new VarOp().ex(a)); }

}
