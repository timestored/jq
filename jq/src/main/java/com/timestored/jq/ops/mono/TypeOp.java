package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jq.ops.Op;

public class TypeOp extends MonadReduceToShort {
	public static TypeOp TYPE = new TypeOp();
	@Override public String name() { return "type"; }
    
	public short type(Object o)  { return (short) run(o); }

    @Override public short ex(boolean a) { return -1; }
	@Override public short ex(byte a)    { return -4; }
    @Override public short ex(short a)   { return -5; }
    @Override public short ex(int a)     { return -6; }
    @Override public short ex(long a)    { return -7; }
    @Override public short ex(float a)   { return -8; }
    @Override public short ex(double a)  { return -9; }
    @Override public short ex(char a)    { return -10; }
	@Override public Short ex(String a)  { return -11; }
    @Override public Object ex(Timstamp m) { return (short) -12; }
    @Override public Object ex(Month dt)    { return (short) -13; }
    @Override public Object ex(Dt dt)    { return (short) -14; }
    @Override public Object ex(Timespan m) { return (short) -16; }
    @Override public Object ex(Minute m) { return (short) -17; }
    @Override public Object ex(Second s) { return (short) -18; }
    @Override public Object ex(Time t)   { return (short) -19; }

    @Override public Short ex(ObjectCol a) 	{ 	return 0; }
    @Override public short ex(BooleanCol a) { 	return 1; }
    @Override public short ex(ByteCol a) 	{ 	return 4; }
    @Override public short ex(ShortCol a) 	{ 	return 5; }
    @Override public short ex(IntegerCol a) { 	return a.getType(); }
    @Override public short ex(LongCol a)   	{ 	return a.getType(); }
    @Override public short ex(FloatCol a)  	{ 	return 8; }
    @Override public short ex(DoubleCol a) 	{ 	return a.getType(); }
    @Override public short ex(CharacterCol a) { return 10; }
    @Override public Short ex(StringCol a) 	{ 	return 11; }
    @Override public Object ex(Op op) 		{ return op.typeNum(); }
    @Override public Object ex(Mapp o) 		{ return (short) 99; }
    @Override public Object ex(Tbl o) 		{ return (short) 98; }

    public Object handleNesting(Object[] o)  { 	return (short) 0; }

	public static boolean isNotList(Object a) {
		short typeNum = TypeOp.TYPE.type(a);
		return typeNum < 0 || typeNum>= 100;
	}
}
