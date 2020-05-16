package com.timestored.jdb.codegen;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import com.google.common.io.Files;
import com.timestored.jdb.database.CType;

import lombok.Getter;

/**
 * Generates Cols, Iterators and Functions for all the types required to allow the database to work.  
 */
public class Genie {

	private static File PROJECT_ORIGIN = new File("C:\\Users\\ray\\dev\\workspace\\jq\\jdb");
	private static File PROJECT_TARGET = new File("C:\\Users\\ray\\dev\\workspace\\jq\\jdb\\build\\generated-src");
	public static final Charset CS = Charset.forName("UTF-8");
	
	@Getter private final File packageFile;
	@Getter private String packageName;

	public static void main(String... args) throws IOException {
		if(args.length > 0) {
			PROJECT_ORIGIN = new File(args[0], "jdb");
			PROJECT_TARGET = new File(PROJECT_ORIGIN,"build" + File.separator + "generated-src");
		}
		ColGenie.generate();
		IteratorGenie.generate();
		PredicateGenie.generate();
		FunctionGenie.generate();
		Genie2.main(new File(PROJECT_ORIGIN, "..").getAbsolutePath());
	}

	
	public Genie(String packageName) {
		this.packageName = packageName;
		String a = ("src.main.java." + packageName + ".").replace(".", File.separator);
		this.packageFile = new File(PROJECT_ORIGIN, a);
	}
	
	private String addHeader(String s) {
		final String HEADER_PRE = "/** This code was generated using code generator:";
		final String HEADER_POST = "**/\r\n";
		return HEADER_PRE + packageName + HEADER_POST + s;
	}
	
	public void writeFile(String newFileName, String contents) throws IOException {
		Path nestingPath = this.PROJECT_ORIGIN.toPath().relativize(packageFile.toPath());
		File destFolder = new File(PROJECT_TARGET, nestingPath.toString());
		destFolder.mkdirs();
		File destFile = new File(destFolder, newFileName);
		System.out.println("Writing: " + destFile);
		Files.write(addHeader(contents), destFile, CS);	
	}
	

	
	public void saveTransformedDoubleFile(File srcDoubleFile, Collection<CType> ctypes) throws IOException {
		saveTransformedDoubleFile(srcDoubleFile, ctypes, Collections.emptyMap());
	}
	

	/**
	 *  Take an existing xxxDoublexxx.java file and convert it, then save it for all the types passed in.
	 *  @param specialIntTransform A transform that is only ran for outputting Int type
	 *          as some functions are called int, some integer.
	 *  @param srcDoubleFile A file that may include type specific comments that are replaced {@see #replaceTypeSpecificComments(CType, String)}
	 */ 
	public void saveTransformedDoubleFile(File srcDoubleFile, Collection<CType> ctypes, Map<CType,Function<String,String>> specialTransforms) throws IOException {
		saveTransformedFile(CType.DOUBLE, srcDoubleFile, ctypes, specialTransforms);
	}


	/**
	 *  Take an existing xxxDoublexxx.java file and convert it, then save it for all the types passed in.
	 *  @param specialIntTransform A transform that is only ran for outputting Int type
	 *          as some functions are called int, some integer.
	 *  @param srcFile A file that may include type specific comments that are replaced {@see #replaceTypeSpecificComments(CType, String)}
	 */ 
	public void saveTransformedFile(CType cType, File srcFile, Collection<CType> ctypes, Map<CType,Function<String,String>> specialTransforms) throws IOException {
		System.out.printf("srcFile = " + srcFile);
		String s = Files.toString(srcFile, Charset.defaultCharset());
		
		for(CType typ : ctypes) {
			if(!typ.equals(cType)) {
				String transformedSrc = s;
				// special case as relying on call from java memoryByteBuffer
				Function<String, String> transform = specialTransforms.get(typ);
				if(transform != null) {
					transformedSrc = transform.apply(s);
				}
				
				transformedSrc = replaceTypeSpecificComments(typ, transformedSrc);
				
				String n = cType.name();
				String Name = n.substring(0, 1).toUpperCase() + n.substring(1).toLowerCase();
				transformedSrc = transformedSrc
									.replace(Name, typ.getLongJavaName())
									.replace(n.toLowerCase(), typ.getNativeJavaName())
									.replace(n.toUpperCase(), typ.getLongJavaName().toUpperCase());
				
				String newFileName = srcFile.getName().replace(Name, typ.getLongJavaName());
				writeFile(newFileName, transformedSrc);
			}
		}
	}
	
	public final static String PRE = "/**TYPE=";
	public final static String POST = "**/";
	
	/**
	 * Given type specific escape sequences in text e.g. **TYPE=INTEGER blah **
	 * remove them for the other types and uncomment them for that specific type.
	 */
	static String replaceTypeSpecificComments(CType typ, String src) {
		String s = src;
		while(s.contains(PRE)) {
			int p = s.indexOf(PRE);
			int q = s.indexOf(POST, p+1);
			if(p != -1 && q != -1) {
				String tmp  = s.substring(p + PRE.length());
				if(tmp.indexOf(" ") == -1) {
					throw new IllegalStateException("found TYPE= but no space delimiting after type");
				}
				String typeSpecified = tmp.substring(0, tmp.indexOf(" "));
				if(typ.name().equalsIgnoreCase(typeSpecified)) {
					// make comment proper code
					s = s.substring(0, p) 
						+ s.substring(p + PRE.length() + typeSpecified.length(),  q).trim()
						+ s.substring(q + POST.length());
				} else {
					// remove comment entirely
					s = s.substring(0, p) 
						+ s.substring(q + POST.length());
				}
			} else if(q == -1) {
				int start = Math.max(0, p - 10);
				int end = Math.min(s.length(), p + 10);
				String txt = s.substring(start, p) + ">" + s.substring(p + 1, end);
				throw new IllegalStateException("no ending found text='" + txt);
			}
		}
		return s;
	}
}
