package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jdb.kexception.NamedException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.BaseDiad;
import com.timestored.jq.ops.SublistOp;
import com.timestored.jq.ops.XkeyOp;

/** RENAME columns in unkeyed table **/
public class XcolOp extends BaseDiad {
	public static XcolOp INSTANCE = new XcolOp();
	@Override public String name() { return "xcol"; }

	@Override public Object run(Object a, Object b) {
		StringCol kcols = XkeyOp.toStringCol(a, new TypeException("first argument must be a symbol list"));
		try {
			if(b instanceof Tbl) {
				return ex(kcols, (Tbl) b);
			} else if(QqtOp.INSTANCE.ex(b)) {
				Mapp m = (Mapp)b;
				Tbl tblA = (Tbl) m.getKey();
				int numAcols = tblA.getKey().size();
				if(numAcols >= kcols.size()) {
					return new MyMapp(ex(kcols,tblA), m.getValue());
				} else {
					StringCol acols = (StringCol) SublistOp.INSTANCE.ex(numAcols, kcols);
					StringCol bcols = (StringCol) SublistOp.INSTANCE.ex(numAcols-kcols.size(), kcols);
					return new MyMapp(ex(acols, tblA), ex(bcols, (Tbl) m.getValue()));
				}
			}
		} catch (IOException e) {
			throw new OsException(e);
		}
		throw new LengthException("xcols only works on unkeyed tables.");
	}

	public Tbl ex(StringCol a, Tbl srcTbl) throws IOException {
		StringCol cNames = srcTbl.getKey();
		int sz = cNames.size();
		if(a.size() > sz) {
			throw new LengthException();
		}
		StringCol sc = new MemoryStringCol(sz);
		ObjectCol oc = new MemoryObjectCol(sz);
		for(int i = 0; i<sz; i++) {
			sc.set(i, i < a.size() ? a.get(i) : cNames.get(i));
			oc.set(i, srcTbl.getValue().get(i));
		}
		return new MyTbl(sc, oc);
	}
}
