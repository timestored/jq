package com.timestored.jq.ops;

import java.io.IOException;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.QsOp;
import com.timestored.jq.ops.mono.TypeOp;

public class SvOp extends BaseDiad {
	public static SvOp INSTANCE = new SvOp(); 
	@Override public String name() { return "sv"; }

	@Override public Object run(Object a, Object b) {
		try {
	    	short ta = TypeOp.TYPE.type(a);
	    	short ata = (short) Math.abs(ta);

	    	// String or char, lists.
	    	if(ata==10 || ata==11) {
				if(b instanceof ObjectCol) {
					if(a instanceof CharacterCol) {
							return ex((CharacterCol)a, (ObjectCol)b);
					} else if(a instanceof Character) {
						return ex((Character)a, (ObjectCol)b);
					} else if(a instanceof String && ((String)a).length()==0) {
						return ex(ColProvider.toCharacterCol(QsOp.NL), (ObjectCol)b);
					}
				} else if(b instanceof StringCol && a instanceof String && ((String)a).length()==0) {
					return ex((StringCol)b);
				}
			/*
			 * Number on left means raise to that power  
			 * 10 sv 2 3 5 7
			 * 2357
			 */
	    	} else if(ta<0 && ta>-20) {
	    		
	    	}
		} catch (IOException e) { }
		throw new TypeException();
	}

	private static final String toS(Object o) {
		if(o instanceof CharacterCol) {
			return CastOp.CAST.s((CharacterCol)o);
		}
		throw new TypeException();
	}


	public String ex(StringCol strings) throws IOException {
		if(strings.size() == 1) {
			return strings.get(0);
		} else if(strings.size() > 1) {
			String sep = strings.get(0).startsWith(":") ? "/" : ".";
			StringBuilder sb = new StringBuilder(strings.get(0));
			for(int i=1; i<strings.size(); i++) {
				sb.append(sep).append(strings.get(i));
			}
			return sb.toString();
		}
		throw new TypeException();
	}
	
	public CharacterCol ex(Character separator, ObjectCol strings) throws IOException {
		return ex(ColProvider.toCharacterCol(""+separator), strings);
	}
	
	/** "," sv ("aa";"bb") **/
	public CharacterCol ex(CharacterCol separator, ObjectCol strings) throws IOException {
		if(strings.size() == 0) {
			return ColProvider.emptyCharacterCol;
		} else if(strings.size() == 1) {
			Object o = strings.get(0);
			if(o instanceof CharacterCol) {
				return (CharacterCol) o;
			}
			throw new TypeException();
		}
		StringBuilder sb = new StringBuilder((strings.size()+separator.size())*2);
		sb.append(toS(strings.get(0)));
		String sep = CastOp.CAST.s(separator);
		for(int i=1; i<strings.size(); i++) {
			sb.append(sep).append(toS(strings.get(i)));
		}
		return ColProvider.toCharacterCol(sb.toString());
	}


	
}