package com.timestored.jdb.codegen;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.timestored.jdb.database.CType;

public class IteratorGenie {

	private static final Genie g = new Genie("com.timestored.jdb.iterator");


	/**
	 * Based on the .java files for the Double versions, generate the other types by replacing strings.  
	 */
	public static void generate() throws IOException {
		// transform the iterators
		for(String fn : new String[] { "EmptyDoubleIter", "ObjectMappedDoubleInter" }) {
			saveTransform(CType.DOUBLE, CType.allTypes(), fn);
		}

		// Can only create these for builtin types as rely on native values for iterators
		List<CType> typs = CType.builtinTypes().stream().filter(t -> !t.equals(CType.BOOLEAN)).collect(Collectors.toList());
		for(String fn : new String[] { "ColDoubleIter", "DoubleIter", "DoubleIterRange" }) {
			saveTransform(CType.DOUBLE, typs, fn);
		}

		// cant create a range of strings :S
		for(String fn : new String[] { "StringIter" }) {
			saveTransform(CType.STRING, Lists.newArrayList(CType.MAPP,CType.OBJECT,CType.MINUTE,CType.SECOND,CType.TIME,
					CType.MONTH,CType.TIMESPAN,CType.TIMSTAMP), fn);
		}
	}


	private static void saveTransform(CType cType, Collection<CType> ctypes, String srcClassName) throws IOException {
		File f = new File(g.getPackageFile(), srcClassName + ".java");
		Map<CType, Function<String, String>> transformMap = Maps.newHashMap();
		transformMap.put(CType.INTEGER, s -> s.replace("DoubleArrayList", "IntArrayList"));
		transformMap.put(CType.CHARACTER, s -> s.replace("DoubleArrayList", "CharArrayList"));
		g.saveTransformedFile(cType, f , ctypes, transformMap);
	}

	public static void main(String[] args) throws IOException {
		IteratorGenie.generate();
	}
}