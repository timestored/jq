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

public class CountOp extends MonadReduceToLong {
	public static final CountOp INSTANCE = new CountOp();
	@Override public String name() { return "count"; }
	public long count(Object o) { return (long) run(o); }
	
    public Long ex(String a)  { return 1l; }
    public long ex(boolean a) { return 1; }
    public long ex(char a)    { return 1; }
    public long ex(short a)   { return 1; }
    public long ex(int a)     { return 1; }
    public long ex(long a)    { return 1; }
    public long ex(float a)   { return 1; }
    public long ex(double a)  { return 1; }
    @Override public Object ex(Dt dt)		{return 1l; }
    @Override public Object ex(Timespan tm) {return 1l; }
    @Override public Object ex(Timstamp tm) {return 1l; }
    @Override public Object ex(Minute tm)	{return 1l; }
    @Override public Object ex(Month tm)	{return 1l; }
    @Override public Object ex(Op op)		{return 1l; }    
    @Override public Object ex(Second tm)	{return 1l; }
    @Override public Object ex(Time tm)		{return 1l; }
    @Override public long ex(byte a) 		{ return 1; }
	
    @Override public Object ex(Tbl o)		{ return run(o.getValue().first()); }
    @Override public Object ex(Mapp o)		{ return (long) o.getKey().size(); }
    
    
	@Override public Long ex(StringCol a)	{ return (long) a.size();}
	@Override public Long ex(ObjectCol a)	{ return (long) a.size();}
	@Override public long ex(DoubleCol a)	{ return a.size();}
	@Override public long ex(CharacterCol a){ return a.size();}
	@Override public long ex(BooleanCol a)	{ return a.size();}
	@Override public long ex(ShortCol a)	{ return a.size();}
	@Override public long ex(IntegerCol a)	{ return a.size();}
	@Override public long ex(FloatCol a)	{ return a.size();}
	@Override public long ex(LongCol a)		{ return a.size();}
	@Override public long ex(ByteCol a)		{ return a.size();}
}
