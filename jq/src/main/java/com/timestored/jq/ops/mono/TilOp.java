package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.database.LimitException;
import com.timestored.jq.TypeException;

public class TilOp extends BaseMonad {
	public static TilOp INSTANCE = new TilOp();
	@Override public String name() { return "til"; }
	
    public LongCol til(long n) {
    	if(CType.isNull(n) || n<0) {
    		throw new DomainException("Can't til negatives or nulls");
    	}
        if(n > Integer.MAX_VALUE) {
            throw new LimitException("can't til over 2 billion");
        }
        return range(0, (long)n);
    }

    public LongCol range(long bottom, long top) {
    	LongCol r = ColProvider.j((int) (top - bottom), i -> (long) i);
        if(Database.QCOMPATIBLE) {
            r.setSorted(false); // TODO remove this eventually.
        }
        return r;
    }

    public LongCol til(int n) {  
		if(CType.isNull(n) || n<0) {
			throw new DomainException("Can't til negatives or nulls");
		}
		return til((long) n); 
	}

    public LongCol til(short n) {  
		if(CType.isNull(n) || n<0) {
			throw new DomainException("Can't til negatives or nulls");
		}
		return til((long) n); 
	}

    public LongCol til(boolean n) { return til(n ? 1l : 0); }  

    public IntegerCol range(int bottom, int top) {
        return ColProvider.i(top - bottom, i -> i);
    }

	@Override public Object run(Object a) {
		if(a instanceof Long) {
			return til((long) a);
		} else if(a instanceof Integer) {
			return til((int) a);
		} else if(a instanceof Short) {
			return til((short) a);
		} else if(a instanceof Boolean) {
			return til((boolean) a);
		}
		throw new TypeException("Expected whole number. Got: " + a.toString());
	}
}
