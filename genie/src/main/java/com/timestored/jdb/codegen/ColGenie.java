package com.timestored.jdb.codegen;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.timestored.jdb.codegen.Genie;
import com.timestored.jdb.database.CType;

public class ColGenie {

	private static final Genie g = new Genie("com.timestored.jdb.col");

	/**
	 * Based on the .java files for the Double versions, generate the other types by replacing strings.  
	 */
	public static void generate() throws IOException {

		Function<String,String> transformChar = (String s) -> s.replace("mbb.getDouble", "mbb.getChar")
																.replace("mbb.putDouble", "mbb.putChar");
		Function<String,String> transformInt = (String s) -> s.replace("mbb.getDouble", "mbb.getInt")
																.replace("mbb.putDouble", "mbb.putInt")
																.replace("stream.readDouble", "stream.readInt")
																.replace("stream.writeDouble", "stream.writeInt");
		Function<String,String> transformByte = (String s) -> s.replace("mbb.getDouble", "mbb.get")
				.replace("mbb.putDouble", "mbb.put");
		Function<String,String> transformString = (String s) -> s.replace("itA.nextDouble() != itB.nextDouble()", "!Objects.equal(itA.nextString(), itB.nextString())");
		transformString = transformString.compose(transformInt);
		
		Map<CType, Function<String, String>> transformMap = Maps.newHashMap();
		transformMap.put(CType.CHARACTER, transformChar);
		transformMap.put(CType.INTEGER, transformInt);
		transformMap.put(CType.BYTE, transformByte);
		transformMap.put(CType.STRING, transformString);
		
		for(String fn : new String[] { "DoubleCol" }) {
			File dFile = new File(g.getPackageFile(), fn + ".java");
			g.saveTransformedDoubleFile(dFile , CType.allTypes(), transformMap);
		}
		
		// The Disk columns should only be generated for native types. StringCOl is actually backed by an IntCol.

		List<CType> typs = CType.builtinTypes().stream().filter(t -> !t.equals(CType.BOOLEAN)).collect(Collectors.toList());
		for(String fn : new String[] { "DiskDoubleCol", "BaseDoubleCol", "MemoryDoubleCol"}) {
			File dFile = new File(g.getPackageFile(), fn + ".java");
			g.saveTransformedDoubleFile(dFile , typs, transformMap);
		}
	}
	
	public static void main(String[] args) throws IOException {
		ColGenie.generate();
	}
}
