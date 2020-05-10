package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.Op;

public abstract class TrimOpBase extends MonadReduceToSameObject {

	@Override public ObjectCol ex(ObjectCol a) { return mapEach(a); }

	@Override public StringCol ex(StringCol a) {
		return a.map(s -> ex(s));
	}

	@Override public String toString() { return Op.toString(this); }
	public abstract String ex(String a);

	@Override public boolean ex(boolean a) { return a; }
    @Override public BooleanCol ex(BooleanCol a) { return a; }
	@Override public byte ex(byte a) { return a; }
    @Override public ByteCol ex(ByteCol a) { return a; }
	@Override public char ex(char a) { if(CType.isNull(a)) { throw new TypeException(); }; return a; }
	@Override public long ex(long a)   { if(CType.isNull(a)) { throw new TypeException(); }; return a; }  
	@Override public float ex(float a)   { if(CType.isNull(a)) { throw new TypeException(); }; return a; }  
	@Override public int ex(int a)   { if(CType.isNull(a)) { throw new TypeException(); }; return a; }  
	@Override public short ex(short a)   { if(CType.isNull(a)) { throw new TypeException(); }; return a; }  
	@Override public double ex(double a)   { if(CType.isNull(a)) { throw new TypeException(); }; return a; }
}
