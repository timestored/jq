package com.timestored.jq.ops.mono;

import java.text.NumberFormat;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jdb.kexception.KException;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.EachOp;
import com.timestored.jq.ops.EqualOp;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.Op;
import com.timestored.jq.ops.SublistOp;

import lombok.Getter;
import lombok.Setter;

import static com.timestored.jq.ops.mono.Qs1Op.B3;

public class QsOp extends MonadReduceToString {
	public static QsOp INSTANCE = new QsOp(); 
	@Override public String name() { return ".Q.s"; }
	public static String NL = "\r\n";
	@Getter @Setter private int precision = 7;
	@Getter private int rows = 25;
	@Getter private int columns = 80;
	
	@Override public Object run(Object o) {
		return ColProvider.toCharacterCol(((String) asText(o)));
	}
	
	public String asText(Object k) {
		try {
			if(k instanceof NiladicOp || k == null) {
				return "";
			}
			long c = CountOp.INSTANCE.count(k);
			if(c == 0 && (k instanceof ObjectCol || (k instanceof Mapp && !(k instanceof Tbl)))) {
				return "";
			} else if(k instanceof RuntimeException) {
				return ex((RuntimeException) k) + NL;
			} 
			String s = ((String) super.run(k));
			if(!s.contains(NL) && s.length() >= columns) {
				s = s.substring(0, columns-3) + "..";
			}
			return s + NL;
		} catch(RuntimeException r) {
			return ex(r) + NL;
		}
	}

	 @Override public String ex(double a) { return B3.ex(a); }
	 @Override public String ex(float a) {  return B3.ex(a); }
	 @Override public String ex(long a) { 	return B3.ex(a); }
	 @Override public String ex(int a) { return B3.ex(a); }
	 @Override public String ex(boolean a) {return B3.ex(a); }
	 @Override public String ex(short a) { 	return B3.ex(a); }
	 @Override public String ex(char a) { 	return B3.ex(a); }
	 @Override public String ex(String a) { return B3.ex(a); }
	 @Override public String ex(byte a) { 	return B3.ex(a); }
	 
	 private String att(Col c) { 
		 return c.isSorted() && c.size()>0 ? "`s#" : "";
	 }

	 @Override public String ex(BooleanCol a) 	{ return B3.ex(a); }
	 @Override public String ex(ByteCol a)    	{ return B3.ex(a); }
	 @Override public String ex(FloatCol a) 	{return B3.ex(a); }
	 @Override public String ex(DoubleCol a) 	{ return B3.ex(a); }
	 @Override public String ex(IntegerCol a) 	{ return B3.ex(a); }
	 @Override public String ex(ShortCol a) 	{return B3.ex(a); }
	 @Override public String ex(LongCol a) 		{ return B3.ex(a); }
	 
	 @Override public String ex(CharacterCol a){
		 return att(a) + (a.size()==1 ? "," : "") + "\"" + unEscapeString(CastOp.CAST.s(a)) + "\""; 
	}
	 
	 
	private final static String ZEROES = "000";
	public static String unEscapeString(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			int ci = (int) c;
			switch (c) {
			case '\n': sb.append("\\n"); break;
			case '\t': sb.append("\\t"); break;
			case '\r': sb.append("\\r"); break;
			case '\b': sb.append("\\b"); break;
			case '\f': sb.append("\\f"); break;
			case '\'': sb.append("\\'"); break;
//			case '\"': sb.append("\\\""); break;
			case '\\': sb.append("\\\\"); break;
			default: 
				if(ci < 32 || ci>256) {
					String hx = Integer.toHexString((int) c);
					sb.append("\\").append(ZEROES.substring(hx.length())).append(hx);
				} else {
					sb.append(c);
				}
			}
		}
		return sb.toString();
	}
	 
	 @Override public String ex(StringCol a) { 	return B3.ex(a); }
	
	 @Override public String ex(RuntimeException r) {
		if(r instanceof KException) {
			return "'" + ((KException)r).getTitle() + (Database.QCOMPATIBLE ? "" : " : " + r.getLocalizedMessage());
		} else {
			return "'" + r.getLocalizedMessage();
		}
	}
	    
    @Override public Object ex(Op op) { return NiladicOp.INSTANCE.equals(op) ? "" : (B3.ex(op)); };


	
	@Override public Object ex(Tbl o) {
		int rowsShown = Math.min(o.size()+2, rows-2);
		return generateTblStrings(new SBarray(rowsShown, columns), o).toString();
	}

	private SBarray generateTblStrings(SBarray sba, Tbl o) {
		StringCol hdr = o.getKey();
		ObjectCol cval = o.getValue();
		int rowsShown = Math.min(o.size()+2, sba.lines.length);
		
		// Go across columns until finished or too wide
		for(int c=0; c<hdr.size(); c++) {
			sba.lines[0].append(hdr.get(c));
			sba.render((Col) cval.get(c), 2);
			// padding between columns and below header
			int desiredWidth = sba.getMaxWidth() + (c < hdr.size()-1 ? 1 : 0);  
			while(sba.lines[1].length() < desiredWidth) { sba.lines[1].append("-"); }
			sba.padtoWidest();
		}
		if(o.size() > rowsShown-2) {
			sba.setLastLine("..");
		}
		return sba;
	}

	/** Array of StringBuilders to allow building row/column out put efficiently **/
	private static class SBarray {
		final StringBuilder[] lines;
		private final int columns;
		public SBarray(int rowsShown, int columns) {	
			lines = new StringBuilder[rowsShown];
			this.columns = columns;
			for(int i = 0; i<rowsShown; i++) { lines[i] = new StringBuilder(columns); }
		}
		
		public void setLastLine(String s) {
			int idx = lines.length-1;
			lines[idx].setLength(0);
			lines[idx].append(s);
		}

		public void padtoWidest() {
			int w = getMaxWidth();
			for(StringBuilder sb : lines) {
				while(sb.length() < w) { sb.append(" "); }
			}
		}

		public void appendAll(String s) {
			for(StringBuilder sb : lines) { sb.append(s); }
		}

		public int getMaxWidth() {
			int maxLineWidth = 0;
			for(StringBuilder sb : lines) {
				maxLineWidth = Math.max(maxLineWidth, sb.length());
			}
			return maxLineWidth;
		}
		
		public void render(Col c, int offset) {
			Object colChars = toStringy22(c);
			for(int r=offset; r < lines.length; r++) {
				String colSt = CastOp.CAST.s((CharacterCol) IndexOp.INSTANCE.run(colChars, r-offset));
				lines[r].append(colSt);
			}
		}

		public void render(Col c) { render(c, 0, o -> toStringy22(o));}

		public void render(Col c, int offset, Function<Object,Object> renderer) {
			Object colChars = renderer.apply(c);
			for(int r=offset; r < lines.length; r++) {
				String colSt = CastOp.CAST.s((CharacterCol) IndexOp.INSTANCE.run(colChars, r-offset));
				lines[r].append(colSt);
			}
		}
		
		private static Object toStringy22(Object k) {
			if(TypeOp.TYPE.type(k) == 0) {
				return EachOp.INSTANCE.run(Qs1Op.B3, k);
			}
			return StringOp.INSTANCE.run(k);
		}
		
		@Override public String toString() {
			StringBuilder superb = new StringBuilder(lines.length * 80);
			for(int i=0; i<lines.length; i++) {
				StringBuilder l = lines[i];
				if(l.length() >= columns) {
					l.setLength(columns-3);
					l.append("..");
				}
				superb.append(l).append(i == lines.length-1 ? "" : NL);
			}
			return superb.toString();
		}
	}

	/**
	 * IF all items in the ObjectCol are of the same atomic or list type, return that typeNum otherwise return 0. 
	 */
	private short getOverallPositiveType(ObjectCol oc) {
		short t = 0;
		if(oc.size() > 0) {
			t = (short) Math.abs(TypeOp.TYPE.type(oc.get(0)));
			for(int i=1; i<oc.size(); i++) {
				short latestT = (short) Math.abs(TypeOp.TYPE.type(oc.get(i)));
				if(latestT != t) {
					return 0;
				}
			}
		}
		return t;
	}
	
	 @Override public String ex(ObjectCol col) {
		 	if(col.size() == 0) {
		 		return "";
		 	}
			int rowsShown = Math.min(col.size(), rows-2);
			SBarray sba = new SBarray(rowsShown, columns);
			
			// isSquare - i.e. type=0 and all items have same count >1.
			short overallType = getOverallPositiveType(col);
			if(FlipOp.getSquareCount(col) != 0 && 10 !=overallType) {
				// TODO check max length
				Col colsToShow = SublistOp.INSTANCE.ex(rows, col);
				ObjectCol newCol = FlipOp.INSTANCE.flip(col);
				for(int c=0; c<newCol.size(); c++) {
					sba.render((Col) newCol.get(c));
					sba.padtoWidest();
					if(c < newCol.size()-1) {
						sba.appendAll(" ");
					}
					if(sba.getMaxWidth() > columns) {
						break;
					}
				}
				return sba.toString();
			}
			// TODO check max length

			if(StringOp.isAllEmptyObjectCol(col)) {
					return (String) StringOp.INSTANCE.run(col);
			}
			sba.render(col, 0, o -> EachOp.INSTANCE.run(Qs1Op.B3, o));
	    	return sba.toString();
	 }
	 
	@Override public String ex(Mapp m) {
		if(m.size() == 0) {
			return "";
		} else if(m.isKeyedTable()) {
			int rowsShown = Math.min(m.size()+2, rows-2);
			SBarray sba = new SBarray(rowsShown, columns);
			generateTblStrings(sba, (Tbl) m.getKey());
			sba.appendAll("| ");
			generateTblStrings(sba, (Tbl) m.getValue());
			return sba.toString();
		}

		int rowsShown = Math.min(m.size(), rows-2);
		SBarray sba = new SBarray(rowsShown, columns);
		sba.render(m.getKey());
		sba.padtoWidest();
		sba.appendAll("| ");
		sba.render(m.getValue());
		if(m.size() > rowsShown) {
			sba.setLastLine("..");
		}
		return sba.toString(); 
	}


	@Override public Object ex(Timstamp dt) { return B3.asString(dt); };
	@Override public Object ex(Timespan dt) { return B3.asString(dt); };
	@Override public Object ex(Dt dt) { return B3.asString(dt); };
	@Override public Object ex(Time tm) { return B3.asString(tm); };
	@Override public Object ex(Second sec) { return B3.asString(sec); };
	@Override public Object ex(Month month) { return B3.asString(month); };
	@Override public Object ex(Minute minute) { return B3.asString(minute); }

	public void setConsole(int rows, int columns) {
		Preconditions.checkArgument(rows >= 8);
		Preconditions.checkArgument(columns >= 8);
		this.rows = rows;
		this.columns = columns;
	}

}
