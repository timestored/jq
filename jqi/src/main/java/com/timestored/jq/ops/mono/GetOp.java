package com.timestored.jq.ops.mono;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.IpcDataReader;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class GetOp extends BaseMonad {
	public static GetOp INSTANCE = new GetOp();

	@Override public Object run(Object a) {
		if(a instanceof String) {
			String id = (String) a;
	    	if(id.startsWith(":")) {
				String fp = id.substring(1).replace("/", File.separator);
				File f = new File(fp);
				try(DataInputStream dis = new DataInputStream(new FileInputStream(f))) {
					Object o = IpcDataReader.read(dis);
					return o;
				} catch (IOException e) {
					throw new TypeException(e.toString());
				}
	    	}
			return this.frame.get(id);	
		} else if(a instanceof Character) {
			return context.getJqLauncher().run(String.valueOf(((char)a)));	
		} else if(a instanceof CharacterCol) {
			CharacterCol cc = (CharacterCol) a;
			if(cc.size() == 0) {
				return NiladicOp.INSTANCE;
			}
			return context.getJqLauncher().run(CastOp.CAST.s(cc));	
		} else if(a instanceof ObjectCol) {
			if(((ObjectCol)a).size() == 0) {
				return ColProvider.emptyCol(CType.OBJECT);
			}
		} else if(a instanceof Mapp) {
			return ((Mapp)a).getValue();
		}
		throw new TypeException("Tried to retrieve value for wrong typed: " + a.toString());
	}
	@Override public String name() {return "get"; }
}
