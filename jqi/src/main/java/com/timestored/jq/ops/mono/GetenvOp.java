package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.Database;
import com.timestored.jq.TypeException;

public class GetenvOp extends BaseMonad {
	public static GetenvOp INSTANCE = new GetenvOp();
	@Override public String name() { return "getenv"; }
	
	@Override public Object run(Object a) {
		if(a instanceof String) {
			return ex((String) a);
		} else if(a instanceof StringCol) {
			return ex((StringCol) a);
		}
		if(Database.QCOMPATIBLE && CountOp.INSTANCE.count(a) == 0) {
			return ColProvider.emptyObjectCol; 
		}
		throw new TypeException();
	}

	public ObjectCol ex(StringCol envNames) {
		return ColProvider.o(envNames.size(), i -> ex(envNames.get(i)));
	}
	
	public CharacterCol ex(String envName) {
		String s = System.getenv(envName);
		return ColProvider.toCharacterCol(s == null ? "" : s);
	}
}
