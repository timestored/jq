package com.timestored.jq.ops;

import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;

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
}
