package com.timestored.jq.ops; 
import com.timestored.jdb.col.*;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.iterator.RangeLocations;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.FirstOp;
import com.timestored.jq.ops.mono.ProjectedOp;
import com.timestored.jq.ops.mono.TypeOp;

public class InOp extends DiadUniformBooleanBase {
	public static InOp INSTANCE = new InOp(); 
	@Override public String name() { return "in"; }
	
	@Override
	public Object run(Object a, Object b) {
		short ta = TypeOp.TYPE.type(a);
		long ca = CountOp.INSTANCE.count(a);
		short tb = TypeOp.TYPE.type(b);
		if(ca == 0) {
			return ta==0 && tb<=0 ? ColProvider.emptyObjectCol : ColProvider.emptyBooleanCol;
		}
		long cb = CountOp.INSTANCE.count(b);
		
		if(cb == 0) {
			return ca == 1 ? false : ColProvider.b((int)ca, false);
		} else if(tb == 0) {
	    	ObjectCol ocb = (ObjectCol)b;
    		// Fir first is atom e.g. 1 2 in (9;10 200;12 13) do each-left
    		if(1<ca && ocb.size()>0 && 1==CountOp.INSTANCE.count(ocb.get(0))) {
    			final ProjectedOp inn = new ProjectedOp(INSTANCE, new Object[] { null, ocb});
    			return EachOp.INSTANCE.run(inn, a);
    		}
        	return NotEqualOp.INSTANCE.ex(ocb.find(a), ocb.size());
	    } else if(ta == 0) {	
			return in((ObjectCol)a, b);
		} else if(ca==1 && cb==1) {
			return EqualOp.INSTANCE.run(a,b);
		}else if(Math.abs(ta) != Math.abs(tb) && Database.QCOMPATIBLE) {
			return ca == 1 ? false : ColProvider.b((int)ca, false);
		} 
		
		// The pattern of this class is to return lists atom+list, we just want to return atom
		Object r = super.run(a, b);
		if(TypeOp.TYPE.type(a) < 0 && r instanceof Col) {
			return FirstOp.INSTANCE.run(r);
		}
		return r;
	}

    @Override public BooleanCol ex(Object a, ObjectCol b) { return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    
    public Object in(ObjectCol a, Object notAnObjectCol) {
		final ProjectedOp inn = new ProjectedOp(INSTANCE, new Object[] { null, notAnObjectCol});
		ObjectCol oc = a.map(f -> inn.run(new Object[] {f}));
		return CastOp.flattenGenericIfSameType(oc);
	}

	@Override public boolean ex(byte a, byte b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(byte a, ByteCol b) { return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
	@Override public BooleanCol ex(ByteCol a, byte b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(ByteCol a, ByteCol b) { return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }
	
	@Override public boolean ex(char a, char b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(char a, CharacterCol b) { return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
	@Override public BooleanCol ex(CharacterCol a, char b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(CharacterCol a, CharacterCol b) { return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }
	
	@Override public boolean ex(String a, String b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(String a, StringCol b) { return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
	@Override public BooleanCol ex(StringCol a, String b) { return  EqualOp.INSTANCE.ex(a, b); }
	@Override public BooleanCol ex(StringCol a, StringCol b) { return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(double a, double b)			{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(double a, DoubleCol b) 	{ return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(DoubleCol a, double b) 	{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(DoubleCol a, DoubleCol b){ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(float a, float b)			{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(float a, FloatCol b) 	{ return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(FloatCol a, float b) 	{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(FloatCol a, FloatCol b)	{ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(short a, short b)			{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(short a, ShortCol b) 	{ return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(ShortCol a, short b) 	{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(ShortCol a, ShortCol b)	{ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(int a, int b)				{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(int a, IntegerCol b) 	{ return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(IntegerCol a, int b) 	{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(IntegerCol a, IntegerCol b)	{ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(long a, long b)			{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(long a, LongCol b) 	{ return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(LongCol a, long b) 	{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(LongCol a, LongCol b){ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    @Override public boolean ex(boolean a, boolean b)		{ return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(boolean a, BooleanCol b) { return new MemoryBooleanCol(b.find(a)!=b.size()) ; }
    @Override public BooleanCol ex(BooleanCol a, boolean b) { return  EqualOp.INSTANCE.ex(a, b); }
    @Override public BooleanCol ex(BooleanCol a, BooleanCol b){ return NotEqualOp.INSTANCE.ex(b.find(a), b.size()); }

    
}