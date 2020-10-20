package com.timestored.jq.ops;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.QsOp;
import com.timestored.jq.ops.mono.TypeOp;

public class VsOp extends BaseDiad {
	public static VsOp INSTANCE = new VsOp(); 
	@Override public String name() { return "vs"; }

	@Override public Object run(Object a, Object b) {
		try {
			if(b instanceof CharacterCol) {
				if(0==CountOp.INSTANCE.count(a)) {
					throw new LengthException();
				} else if(a instanceof Character) {
					return ex((Character)a, (CharacterCol)b);
		    	} else if(a instanceof CharacterCol) {
					return ex((CharacterCol)a, (CharacterCol)b);
		    	} else if(a instanceof String && ((String)a).length()==0) {
					return ex(ColProvider.toCharacterCol(QsOp.NL), (CharacterCol)b);
		    	}
			} else if(a instanceof String && ((String)a).length()==0) {
				if(b instanceof String) {
					return ex((String)b);
				}
	    	}
		} catch (IOException e) { }
		throw new TypeException();
	}

	
	private StringCol ex(String b) {
		if(b.startsWith(":")) {
			int p = b.lastIndexOf("/");
			if(p == -1) {
				return ColProvider.toStringCol(new String[] {b});
			}
			return ColProvider.toStringCol(new String[] { b.substring(0, p), b.substring(p+1) });
		}
		ArrayList<String> al = Lists.newArrayList(Splitter.on(".").split(b));
		return ColProvider.toStringCol(al);
		
	}

	public ObjectCol ex(Character separator, CharacterCol b) throws IOException {
		return ex(ColProvider.toCharacterCol(""+separator), b);
	}
	
	/** "," sv ("aa";"bb") **/
	public ObjectCol ex(CharacterCol separator, CharacterCol st) throws IOException {
		if(st.size() == 0) {
			return MemoryObjectCol.of(ColProvider.emptyCharacterCol);
		} else {
			String s = CastOp.CAST.s(st);
			String sep = CastOp.CAST.s(separator);
			ArrayList<String> al = Lists.newArrayList(Splitter.on(sep).split(s));
			return ColProvider.toCharacterCol(al);
		}
	}


	
}