package com.timestored.jq.ops;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.RankException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.ProjectedOp;
import com.timestored.jq.ops.mono.TypeOp;

public interface Diad extends Op {
    public Object run(Object a, Object b);
    @Override default short typeNum() { return 102; }
	
	public default ObjectCol ex(ObjectCol a, ObjectCol b) {
		if(a.size() != b.size()) {
            throw new RuntimeException("length");
		}
		ObjectCol r = new MemoryObjectCol(a.size());
		for(int i=0; i<a.size(); i++) {
			r.set(i, run(a.get(i),b.get(i)));
		}
		return r;
	}
	
	@Override default int getRequiredArgumentCount() { return 2; }

    @Override default Object run(Object[] args) {
    	if(args.length > 2) {
    		throw new RankException();
    	}
    	if(args.length == 2 && args[0] != null && args[1] != null) {
    		return run(args[0], args[1]);
    	}
    	return new ProjectedOp(this, args); 
    }
    

    public static Object mapEach(Diad d, Object a, Object b) {
    	short ta = TypeOp.TYPE.type(a);
    	short tb = TypeOp.TYPE.type(b);
		long ca = CountOp.INSTANCE.count(a);
		long cb = CountOp.INSTANCE.count(b);
    	if(a instanceof Mapp && b instanceof Mapp) {
    		if(ca == 0 || cb == 0) {
    			return ca;
    		}
        	throw new TypeException();
    	} else if(a instanceof Mapp && tb < 0) {
    		return new MyMapp(((Mapp)a).getKey(), (Col) d.run(((Mapp)a).getValue(), b));
    	} else if(ta < 0 && b instanceof Mapp) {
    		return new MyMapp(((Mapp)b).getKey(), (Col) d.run(b, ((Mapp)b).getValue()));
    	} else if(a instanceof ObjectCol && b instanceof ObjectCol) {
    		ObjectCol oa = (ObjectCol)a;
    		ObjectCol ob = (ObjectCol)b;
    		if(oa.size() == ob.size()) {
        		return ColProvider.o(oa.size(), i -> d.run(oa.get(i), ob.get(i)));
    		}
			throw new LengthException();
    	} else if(a instanceof ObjectCol) {
    		if(ca == 0 && (tb<=0 || cb==0)) {
    			return ColProvider.emptyCol(CType.OBJECT);
    		} else if(cb==1) {
    			return CastOp.flattenGenericIfSameType(((ObjectCol) a).each(o -> d.run(o, b)));
    		}
    	} else if(b instanceof ObjectCol) {
    		if(cb == 0 && (ta<=0 || ca==0)) {
    			return ColProvider.emptyCol(CType.OBJECT);
    		}  else if(ca==1) {
    			return CastOp.flattenGenericIfSameType(((ObjectCol) b).each(o -> d.run(a, o)));
    		}
    	}
    	throw new TypeException();
    }
    
}
