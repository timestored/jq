package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jdb.kexception.NamedException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.BaseDiad;
import com.timestored.jq.ops.XkeyOp;

/** Reorder columns in unkeyed table **/
public class XcolsOp extends BaseDiad {
	public static XcolsOp INSTANCE = new XcolsOp();
	@Override public String name() { return "xcols"; }

	@Override public Object run(Object a, Object b) {
		StringCol kcols = XkeyOp.toStringCol(a, new TypeException("first argument must be a symbol list"));
		if(b instanceof Tbl) {
			try {
				return ex(kcols, (Tbl) b);
			} catch (IOException e) {
				throw new OsException(e);
			}
		} 
		throw new LengthException("xcols only works on unkeyed tables.");
	}

	public Tbl ex(StringCol a, Tbl srcTbl) throws IOException {
		int sz = srcTbl.getKey().size();
		List<String> headers = new ArrayList<>(sz);
		ObjectCol oc = new MemoryObjectCol(sz);
		int i = 0;
		for(String s : a.toStringArray()) {
			headers.add(s);
			Col v = srcTbl.getCol(s);
			if(v == null) { // Column being rearranged never existed in srcTbl
				throw new NamedException(s);
			}
			oc.set(i++, srcTbl.getCol(s));
		}
		for(String s : srcTbl.getKey().toStringArray()) {
			if(!headers.contains(s)) {
				headers.add(s);
				oc.set(i++, srcTbl.getCol(s));	
			}
		}
		return new MyTbl(ColProvider.toStringCol(headers), oc);
	}
}
