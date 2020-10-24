package com.timestored.jq.ops;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jq.Context;
import com.timestored.jq.Frame;

import lombok.Getter;
import lombok.Setter;

public abstract class BaseDiad implements Diad {
	@Getter @Setter protected Frame frame;
	@Getter @Setter protected Context context;

    @Override public short typeNum() { return 102; }
    
	public ObjectCol ex(ObjectCol a, ObjectCol b) {
		if(a.size() != b.size()) {
            throw new RuntimeException("length");
		}
		return ColProvider.o(a.size(), i -> run(a.get(i),b.get(i)));
	}

	@Override public String toString() { return name(); }
}
