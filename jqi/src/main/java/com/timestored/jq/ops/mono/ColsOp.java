package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.Database;
import com.timestored.jq.TypeException;

public class ColsOp extends BaseMonad {
	public static final ColsOp INSTANCE = new ColsOp();
	@Override public String name() { return "cols"; }
	
	@Override public Object run(Object a) { return ex(a); }
	
	public StringCol ex(Object a) {
		if(a instanceof Tbl) {
			return ((Tbl)a).getKey();
		} else if(a instanceof Mapp) {
			Mapp m = ((Mapp)a);
			if(QqtOp.INSTANCE.ex(a)) {
				StringCol colsA = ((Tbl) m.getKey()).getKey();
				StringCol colsB = ((Tbl) m.getValue()).getKey();
				try {
					List<String> l = new ArrayList<>();
					l.addAll(Arrays.asList(colsA.toStringArray()));
					l.addAll(Arrays.asList(colsB.toStringArray()));
					return ColProvider.toStringCol(l);
				} catch (IOException e) {
					throw new TypeException();
				}
			} else if(Database.QCOMPATIBLE) {
				Col k = m.getKey();
				if(k instanceof StringCol) {
					return (StringCol) k;	
				}	
			}
		}
		throw new TypeException();
	}
}
