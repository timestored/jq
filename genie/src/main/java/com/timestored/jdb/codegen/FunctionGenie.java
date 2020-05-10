package com.timestored.jdb.codegen;

import java.io.IOException;

import com.timestored.jdb.codegen.Genie;
import com.timestored.jdb.database.CType;

public class FunctionGenie {

	private static final String PKG = "com.timestored.jdb.function";
	private static final Genie g = new Genie(PKG);

	/**
	 * Generate xxxConsumer,xxxPredicate, ToXxxFunctions for all possible types. 
	 */
	public static void generate() throws IOException {

		final String ST =  "package " + PKG + ";"
				+ "\r\nimport java.sql.Date;"
				+ "\r\nimport com.timestored.jdb.col.Mapp;"
				+ "\r\nimport com.timestored.jdb.col.Col;"
				+ "\r\nimport com.timestored.jdb.database.*;"
				+ "\r\n@FunctionalInterface"
			    + "\r\n";
		
		String consumerSrc = ST + "public interface <BIGTYPE>Consumer {	void accept(<NATIVETYPE> value); }";
		String predicateSrc = ST + "public interface <BIGTYPE>Predicate { boolean test(<NATIVETYPE> value); }";
		String toFunctionSrc = ST + "public interface To<BIGTYPE>Function<T> { <NATIVETYPE> applyAs<BIGTYPE>(T value); }";
		String monadToFunctionSrc = ST + "public interface MonadTo<BIGTYPE>Function { <NATIVETYPE> map(<NATIVETYPE> a); }";
		String diadToFunctionSrc = ST + "public interface DiadTo<BIGTYPE>Function { <NATIVETYPE> map(<NATIVETYPE> a, <NATIVETYPE> b); }";

		//   public abstract double applyAsDouble(java.lang.Object arg0);
		
		writeFilesForAllTypes(consumerSrc, "", "Consumer");
		writeFilesForAllTypes(predicateSrc, "", "Predicate");
		writeFilesForAllTypes(toFunctionSrc, "To", "Function");
		writeFilesForAllTypes(monadToFunctionSrc, "MonadTo", "Function");
		writeFilesForAllTypes(diadToFunctionSrc, "DiadTo", "Function");
	}
	

	private static void writeFilesForAllTypes(String srcCode, String prefix, String postfix) throws IOException {
		for(CType typ : CType.values()) {
			String src = srcCode.replace("<BIGTYPE>", typ.getLongJavaName())
							.replace("<NATIVETYPE>", typ.getNativeJavaName());
			String newFileName = prefix + typ.getLongJavaName() + postfix  + ".java";
			g.writeFile(newFileName , src);
		}
	}
}
