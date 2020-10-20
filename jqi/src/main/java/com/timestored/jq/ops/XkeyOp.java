package com.timestored.jq.ops;

import java.io.IOException;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.ColsOp;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.QqtOp;
import com.timestored.jq.ops.mono.TypeOp;
import com.timestored.jq.ops.mono.XcolsOp;

public class XkeyOp extends BaseDiad {
	public static XkeyOp INSTANCE = new XkeyOp(); 
	@Override public String name() { return "xkey"; }

	@Override public Object run(Object a, Object b) {
		StringCol kcols = toStringCol(a, new TypeException(" first argument must be a symbol list"));
		
		try {
			if(b instanceof Tbl) {
				return ex(kcols, (Tbl)b);
			} else if(QqtOp.INSTANCE.ex(b) && b instanceof Mapp) {
				return ex(kcols, (Tbl) BangOp.INSTANCE.ex(0, b));
			}
		} catch (IOException e) {
			throw new OsException(e);
		}
		throw new TypeException();
	}

	private Object ex(StringCol kcols, Tbl tbl) throws IOException {
		Tbl b = XcolsOp.INSTANCE.ex(kcols, tbl);
		return BangOp.INSTANCE.ex(kcols.size(), b);
	}

	public static StringCol toStringCol(Object a, TypeException e) {
		if(a instanceof StringCol) {
			return (StringCol)a;
		} else if(a instanceof String) {
			return new MemoryStringCol((String)a);
		} else if(a instanceof ObjectCol && 0==CountOp.INSTANCE.count(a)) {
			return ColProvider.emptyStringCol;
		}
		throw e;
	}

	public Tbl unkey(Mapp m) {
		try {
			if(QqtOp.INSTANCE.ex(m)) {
					return (Tbl) BangOp.INSTANCE.ex(0,m);
			}
		} catch (IOException e) { }
		throw new TypeException();
	}
  	
}