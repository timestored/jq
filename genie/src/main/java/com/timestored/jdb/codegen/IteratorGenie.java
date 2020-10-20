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
import com.timestored.jdb.database.CTypeI;

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
		List<CTypeI> typs = CType.builtinTypes().stream().filter(t -> !t.equals(CType.BOOLEAN)).collect(Collectors.toList());
		saveTransform(CType.DOUBLE, typs, "DoubleIterRange");
		typs.add(CType.OBJECT);
		for(String fn : new String[] { "ColDoubleIter", "DoubleIter" }) {
			saveTransform(CType.DOUBLE, typs, fn);
		}

		// cant create a range of strings :S
		for(String fn : new String[] { "StringIter" }) {
			saveTransform(CType.STRING, Lists.newArrayList(CType.MAPP,CType.OBJECT,CType.MINUTE,CType.SECOND,CType.TIME,
					CType.MONTH,CType.TIMESPAN,CType.TIMSTAMP,CType.DT), fn);
		}
	}


	private static void saveTransform(CTypeI cType, Collection<CTypeI> ctypes, String srcClassName) throws IOException {
		File f = new File(g.getPackageFile(), srcClassName + ".java");
		Map<CTypeI, Function<String, String>> transformMap = Maps.newHashMap();
		transformMap.put(CType.INTEGER, s -> s.replace("DoubleArrayList", "IntArrayList"));
		transformMap.put(CType.CHARACTER, s -> s.replace("DoubleArrayList", "CharArrayList"));
		g.saveTransformedFile(cType, f , ctypes, transformMap);
	}

	public static void main(String[] args) throws IOException {
		IteratorGenie.generate();
	}
}