package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jq.Context;
import com.timestored.jq.Frame;
import com.timestored.jq.TypeException;

import lombok.Getter;
import lombok.Setter;

public abstract class BaseMonad implements Monad {
    @Setter @Getter protected Frame frame;
    @Setter @Getter protected Context context;

	@Override public short typeNum() { return 101; }

    public Object ex(RuntimeException o) { throw o; }

    public Object  mapEach(Object o) {
    	if(o instanceof Tbl) {
    		return mapEach((Tbl)o);
    	} else if(o instanceof ObjectCol) {
    		return mapEach((ObjectCol)o);
    	} else if(o instanceof Mapp) {
    		return mapEach((Mapp)o);
    	}
    	throw new TypeException();
    }
    
    public ObjectCol  mapEach(ObjectCol o) {
        if(o.size() == 0) { return o; }
        ObjectCol r = new MemoryObjectCol(o.size());
        for(int i=0; i<r.size(); i++) {
            r.set(i, run(o.get(i)));
        }
        return r;
    }

    public Tbl mapEach(Tbl o) {
        if(o.size() == 0) { return o; }
    	return new MyTbl(o.getKey(), mapEach(o.getValue()));
    }
    
    public Object  mapEach(Mapp o) {
        if(o.size() == 0) { return o; }
        Object ret = run(o.getValue());
        if(TypeOp.TYPE.type(ret) < 0) {
        	return ret;
        } else if(ret instanceof Col) {
    		return new MyMapp(o.getKey(), (Col)ret);
        }
        throw new TypeException("Couldn't reform dictionary from " + ret.toString()); 
    }
	@Override public String toString() { return name(); }
}
