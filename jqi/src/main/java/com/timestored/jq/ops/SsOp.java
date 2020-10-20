package com.timestored.jq.ops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.TypeException;

import lombok.Data;

public class SsOp extends BaseDiad {
	public static SsOp INSTANCE = new SsOp(); 
	@Override public String name() { return "ss"; }

	@Override public Object run(Object a, Object b) {
		try {
			if(a instanceof CharacterCol) {
				CharacterCol cca = (CharacterCol)a;
				if(b instanceof Character) {
					return ex(cca, (Character)b);
				} else if(b instanceof CharacterCol) {
					return ex(cca, (CharacterCol)b);
				}
			}
		} catch (IOException e) { }
		throw new TypeException();
	}
	
	public LongCol ex(CharacterCol haystack, Character needle) throws IOException {
		return ex(haystack, ColProvider.toCharacterCol(""+needle));
	}
	
	private static interface Matcher { boolean isMatch(char c); };
	@Data private static class CMatcher implements Matcher {
		private final char c;
		public boolean isMatch(char c) { return this.c == c; }  
	};
	private static class SetMatcher implements Matcher {
		private final Set<Character> chars = new HashSet<>(5);
		private final boolean negate;
		public SetMatcher(String qregex) {
			negate = qregex.startsWith("^");
			String s = negate ? qregex.substring(1) : qregex;
			for(char c : s.toCharArray()) {
				chars.add(c);
			}
		}
		public boolean isMatch(char c) { return chars.contains(c); }  
	};
	private static class AnyMatcher implements Matcher {
		@Override public boolean isMatch(char c) { return true; }
	}
	
	private static class QMatcher {
		private final Matcher[] matchers;
		
		public QMatcher(String needle) {
			List<Matcher> ms = new ArrayList<>();
			for(int i=0; i<needle.length(); i++) {
				if(needle.charAt(i) == '[') {
					int start = ++i;
					while(i<needle.length() && needle.charAt(i)!=']') { 
						i++;
					}
					if(i == needle.length()) { 
						throw new LengthException("No matching ] found"); 
					}
					ms.add(new SetMatcher(needle.substring(start, i)));
				} else if(needle.charAt(i) == '?'){
					ms.add(new AnyMatcher());
				} else {
					ms.add(new CMatcher(needle.charAt(i)));
				}
			}
			this.matchers = ms.toArray(new Matcher[0]);
		}
		
		boolean isMatch(int index, char c) {
			return matchers[index].isMatch(c);
		}

		public int characterSize() { return matchers.length; }
	}
	
	/** "," sv ("aa";"bb") **/
	public LongCol ex(CharacterCol haystack, CharacterCol needle) throws IOException {
		if(needle.size() == 0) {
			throw new LengthException("Can't search for empty needle");
		} else if(haystack.size() == 0) {
			return ColProvider.emptyLongCol;
		}
		QMatcher qmatcher = new QMatcher(CastOp.CAST.s(needle));
		List<Long> matchPositions = new ArrayList<>();
		int uBound = 1+haystack.size()-qmatcher.characterSize();
		for(int i=0; i<uBound;) {
			int j=0;
			for(;j<qmatcher.characterSize() && qmatcher.isMatch(j, haystack.get(i+j)); j++) {}
			if(j == qmatcher.characterSize()) {
				matchPositions.add((long) i);
				i += j;
			} else {
				i++;
			}
		}
		return ColProvider.j(matchPositions);
	}


	
}