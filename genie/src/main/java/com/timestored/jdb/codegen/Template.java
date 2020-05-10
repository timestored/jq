package com.timestored.jdb.codegen;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;

import lombok.AllArgsConstructor;

/**
 * Simple Template system to allow java code generation by replacing ##namedTags##. 
 */
public class Template {
	
	private static final String POST = "##";
	private static final String PRE = "##";
	private final Map<String,String> substitutions;
	private final Map<String,Function<String,String>> replacements;

	public Template(Map<String, String> substitutions, Map<String, Function<String,String>> replacements) {
		this.substitutions = substitutions == null ? Collections.emptyMap() : substitutions;
		this.replacements = replacements == null ? Collections.emptyMap() : replacements;
		if(substitutions.isEmpty() && replacements.isEmpty()) {
			throw new IllegalArgumentException("No substitutions or replacements specified");
		}
	}

	
	public static String toString(InputStream is) throws IOException {
		return CharStreams.toString(new InputStreamReader(is));
	}
	
	public String apply(String templateString) {
		
		String s = templateString;
		for(String k : substitutions.keySet()) {
			s = s.replace(PRE + k + POST, substitutions.get(k));
		}

		for(String k : replacements.keySet()) {
			String needle = PRE + k + POST;
			int p = s.indexOf(needle);
			while(p != -1) {
				int q = s.indexOf(needle, p + 1);
				String srcText = "";
				if(q != -1) {
					srcText = s.substring(p + needle.length(), q);
					String replacementText = replacements.get(k).apply(srcText);
					String srcIncludingKey = s.substring(p, q + needle.length());
					s = s.replace(srcIncludingKey, replacementText);
				}
				p = s.indexOf(needle);
			}
		}
		
		if(s.contains(PRE)) {
			int pos = s.indexOf(PRE);
			System.out.println(s.substring(pos));
			throw new IllegalStateException("something wasn't replaced");
		}
		
		return s;
	}
	
	
	
	

//	public static <T> Map<String, Template> convertMap(Map<String, Function<T, String>> lookup, 
//			final List<T> vals, String separator) {
//		
//		Map<String, Template> m = Maps.newHashMap();
//		
//		
//		for(String k : lookup.keySet()) {
//			Function<T, String> translator = lookup.get(k);
//			Map<String, String> subsMap = Maps.newHashMap();
//			
//			StringBuilder sb = new StringBuilder();
//			for(T t : vals) {
//				sb.append(translator.apply(t));
//				sb.append(separator);
//			}
//			subsMap.put(k, sb.toString());
//		}
//		Template tm = new Template(subsMap , null);
//			
//		return m;
//	}
	
	
	
	
	
	
	/**
	 * Templating system that allows generating code for lists of different values 
	 * by mapping from names to lookups within that type.
	 */
	public static class TypedTemplate<T> {
		
		private final Collection<T> vals;
		private final Map<String,Function<T,String>> lookups;

		public TypedTemplate(Collection<T> vals, Map<String, Function<T, String>> lookups) {
			this.vals = Preconditions.checkNotNull(vals);
			this.lookups = Preconditions.checkNotNull(lookups);
		}

		public String apply(String tstring) {
			if(tstring == null || tstring.length()==0) {
				throw new IllegalArgumentException("Source tstring was empty?");
			}
			StringBuilder sb = new StringBuilder();
			for(T t : vals) {
				String s = tstring;
				for(String k : lookups.keySet()) {
					s = s.replace("##" + k + "##", lookups.get(k).apply(t));
				}
				sb.append(s);
			}
			return sb.substring(0, sb.length() - 2);
		}

	}
}
