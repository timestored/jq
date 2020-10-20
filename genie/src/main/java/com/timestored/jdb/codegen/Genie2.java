package com.timestored.jdb.codegen;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.timestored.jdb.database.CType;
import static com.timestored.jdb.database.CType.*;
import com.timestored.jdb.database.CTypeI;

import lombok.Getter;


/**
 * Generates Cols, Iterators and Functions for all the types required to allow the database to work.  
 */
public class Genie2 {

	public static final Charset CS = Charset.forName("UTF-8");
	
	@Getter private final File packageFile;
	@Getter private String packageName;
	private final File projectOrigin;
	private final File projectTarget;
	
	public Genie2(String packageName, File projectOrigin, File projectTarget) {
		this.packageName = packageName;
		this.projectOrigin = projectOrigin;
		this.projectTarget = projectTarget;
		String a = (packageName + ".").replace(".", File.separator);
		this.packageFile = new File(projectOrigin, a);
	}
	
	private String addHeader(String s) {
		final String HEADER_PRE = "/** This code was generated using code generator:";
		final String HEADER_POST = "**/\r\n";
		return HEADER_PRE + packageName + HEADER_POST + s;
	}
	
	private void writeFile(String newFileName, String contents) throws IOException {
		Path nestingPath = this.projectOrigin.toPath().relativize(packageFile.toPath());
		File destFolder = new File(projectTarget, nestingPath.toString());
		destFolder.mkdirs();
		File destFile = new File(destFolder, newFileName);
		System.out.println("Writing: " + destFile);
		Files.write(destFile.toPath(), addHeader(contents).getBytes());	
	}
	
	
	
	public static void main(String... args) throws IOException {
		File projectOrigin = new File(args[0], "jqi\\src\\main\\resources\\templates".replace("\\", File.separator));
		File projectTarget = new File(args[0], "jqi\\build\\generated-src\\src\\main\\java\\".replace("\\", File.separator));
		Genie2 genDia = new Genie2("com.timestored.jq.ops", projectOrigin, projectTarget);
		deleteFolder(new File(projectTarget, "com/timestored/jq/ops"));
		generateDiads(genDia);
		Genie2 genMon = new Genie2("com.timestored.jq.ops.mono", projectOrigin, projectTarget);
		generateMonads(genMon);
        
	}

	
	private static void generateDiads(Genie2 g2) throws IOException {
		List<CTypeI> diadTypes = Arrays.asList(BOOLEAN, SHORT, INTEGER, LONG, FLOAT, DOUBLE, CHARACTER, BYTE, STRING);
		Template diadTempl = new Template(Collections.emptyMap(), getPairReplacements(diadTypes, p -> p.a.getTypeNum() > p.b.getTypeNum()));
		String tempName = "DiadXXX.template";
		String src = g2.readFile(tempName);
		String uniSrc = src.replace("##Action##", "Uniform").replace("##returntype##", "##type##").replace("##returnlisttype##", "##listtype##");
		// Horrible hack to change return type for time types dynamically
		uniSrc = uniSrc.replace("return ex(imvA.getInt(), ((IntegerMappedVal)b).getInt());", "int ri = ex(imvA.getInt(), ((IntegerMappedVal)b).getInt()); return CastOp.CAST.run(imvA.getType(), ri);");
		uniSrc = uniSrc.replace("return ex(imvA.getLong(), ((LongMappedVal)b).getLong());", "long rj = ex(imvA.getLong(), ((LongMappedVal)b).getLong()); return CastOp.CAST.run(imvA.getType(), rj);");
		String bdu = diadTempl.apply(uniSrc).replace("public abstract BooleanCol ex", "public abstract Col ex")
				.replace("public abstract boolean ex", "public abstract Object ex")
				.replace("public abstract byte ex", "public abstract Object ex")
				.replace("public abstract ByteCol ex", "public abstract Col ex");
		g2.writeFile("BaseDiadUniform.java", bdu);
		String booleSrc = src.replace("##Action##", "UniformBoolean").replace("##returntype##", "boolean").replace("##returnlisttype##", "BooleanCol");;
		g2.writeFile("BaseDiadUniformBoolean.java", diadTempl.apply(booleSrc));
		
		// Generate Math Ops
		Template mathOpTempl = getTemplate(Arrays.asList(SHORT, INTEGER, LONG, FLOAT, DOUBLE));
		
		// And OrOp
		g2.writeFile("FillOp.java", mathOpTempl.apply(g2.readFile("FillOp.template")));	
		g2.writeFile("AndOp.java", mathOpTempl.apply(g2.readFile("AndOp.template")));	
		g2.writeFile("OrOp.java", mathOpTempl.apply(g2.readFile("AndOp.template").replace("And", "Or").replace("and", "or").replace("min", "max").replace("&&", "||")));	
		
		String mathOpSrc = g2.readFile("MathOp.template");
		// For uniform functions that return same type
		mathOpSrc = mathOpSrc.replace("##returntype##", "##type##").replace("##returnTypeChar##", "##typeChar##").replace("##returnCast##", "##cast##");
		String moSt = mathOpTempl.apply(mathOpSrc);
		String[] ops = new String[] { "*", "%", "+", "*", "/", "-" };
		String[] nam = new String[] { "Mul", "Mod", "Add", "Sub", "NaiveDiv", "Sub" };
		for(int i = 0; i<ops.length; i++) {
			g2.writeFile(nam[i] + "Op.java", moSt.replace("MathOp", nam[i] + "Op").replace("%", ops[i]).replace("\"%\"", "\"mod\"").replace("\"/\"", "\"div\""));
		}
		// For comparison functions that return boolean
		mathOpSrc = g2.readFile("ComparisonOp.template");
		mathOpSrc = mathOpSrc.replace("##returntype##", "boolean").replace("##ReturnType##", "Boolean").replace("##returnCast##", "")
				.replace("KRunnerBinOpBase", "DiadUniformBooleanBase").replace("##returnlisttype##", "BooleanCol");
		moSt = mathOpTempl.apply(mathOpSrc);
		ops = new String[] { "<", "<=", "==", "!=" };
		nam = new String[] { "NaiveLessThan", "NaiveLessThanOrEqual", "NaiveEqual", "NotEqual" };
		for(int i = 0; i<ops.length; i++) {
			g2.writeFile(nam[i] + "Op.java", moSt.replace("ComparisonOp", nam[i] + "Op").replace("%", ops[i]).replace("\"!=\"", "\"<>\""));
		}

		// Generate CastOp
		List<CTypeI> castTypes = Arrays.asList(BOOLEAN, SHORT, INTEGER, LONG, FLOAT, DOUBLE);
		Template castOpTempl = new Template(Collections.emptyMap(), getPairReplacements(castTypes, p -> !p.a.equals(p.b)));
		g2.writeFile("CastOp.java", castOpTempl.apply(g2.readFile("CastOp.template")));
		
		List<CTypeI> nonObjects = CType.ALL_NATIVE_TYPES.stream().filter(dt -> dt.getTypeNum()!=0).collect(Collectors.toList());
		g2.writeFile("IndexOp.java", getTemplate(nonObjects).apply(g2.readFile("IndexOp.template")));
	}

	private static void generateMonads(Genie2 genMon)
			throws IOException {
		List<CTypeI> allTypes = CType.ALL_NATIVE_TYPES.stream().filter(dt -> dt.getTypeNum()!=0 && Math.abs(dt.getTypeNum())!=11).collect(Collectors.toList());
		Template monadTempl = getTemplate(allTypes);
		String tempName = "MonadXXX.template";
		String src = genMon.readFile(tempName);
		String out = src.replace("##returntype##", "##type##").replace("##returnType##", "##Type##")
				.replace("##Action##", "ReduceToSame").replace("##ReturnType##", "Object");
		genMon.writeFile(tempName.replace("XXX", "ReduceToSameObject").replace(".template", ".java"), monadTempl.apply(out));
		
		// Special one to create base
		out = src.replace("public abstract ##returntype## ex(##type## a);", "public ##returntype## ex(##type## a) { return a; };  ");
		out = out.replace("##returntype##", "Object").replace("##returnType##", "Object")
				.replace("##Action##", "ReduceTo").replace("##ReturnType##", "Object");
		genMon.writeFile(tempName.replace("XXX", "ReduceToObject").replace(".template", ".java"), monadTempl.apply(out));
		Iterable<CTypeI> iter = CType.ALL_NATIVE_TYPES.stream().filter(dt -> dt.getTypeNum() < 0)::iterator;
		for(CTypeI toType : iter) {
			String jnam = toType.getLongJavaName();
			out = src.replace("##returntype##", toType.getNativeJavaName()).replace("##returnType##", toType.getLongJavaName())
					.replace("##Action##", "ReduceTo").replace("##ReturnType##", jnam);
			genMon.writeFile(tempName.replace("XXX", "ReduceTo" + jnam).replace(".template", ".java"), monadTempl.apply(out));
		}
		
		

		genMon.writeFile("ReverseOp.java", getTemplate(allTypes).apply(genMon.readFile("ReverseOp.template")));
		List<CTypeI> trimTypes = CType.ALL_NATIVE_TYPES.stream().filter(dt -> { short t = (short) Math.abs(dt.getTypeNum()); return t!=0 && t!=11 && t!=1;}).collect(Collectors.toList());
		genMon.writeFile("TrimOp.java", getTemplate(trimTypes).apply(genMon.readFile("TrimOp.template")));
		genMon.writeFile("LTrimOp.java", getTemplate(trimTypes).apply(genMon.readFile("LTrimOp.template")));
		genMon.writeFile("RTrimOp.java", getTemplate(trimTypes).apply(genMon.readFile("RTrimOp.template")));
		
		String moops = genMon.readFile("MonoDoubleOp.template");
        String[] cmds = { "cos","sin","tan","acos","asin","atan","exp","log", "sqrt", "reciprocal" };
        for(String name : cmds) {
        	String code = name.equals("reciprocal") ? "1/d" : "Math." + name + "(d)";
            String s = moops.replace("$name$", name).replace("$Name$", name).replace("$code$", code);
    		genMon.writeFile(name + "Op.java", s);
        }

        String[] monos = { "NaiveAbs","All","Any","Avg","Var","Not" };
        for(String m : monos) {
        	genMon.writeFile(m+"Op.java", getTemplate(allTypes).apply(genMon.readFile(m+"Op.template")));
        }
    	genMon.writeFile("ColCreator.java", getTemplate(CType.ALL_NATIVE_TYPES).apply(genMon.readFile("ColCreator.template")));
	}

	private static Map<String, Function<String, String>> getPairReplacements(Collection<CTypeI> ctypes, Predicate<Pair<CTypeI, CTypeI>> filter) {
		Map<String, Function<String, String>> replacements = getTypeReplacements(ctypes);
		List<Pair<CTypeI,CTypeI>> castPairs = new ArrayList<>();
        for(CTypeI dType : ctypes) {
            for (CTypeI srcType : ctypes) {
            	Pair<CTypeI, CTypeI> p = new Pair<>(srcType, dType);
            	if(filter.test(p)) {
            		castPairs.add(p);
            	}
            }
        }
        replacements.putAll(getFromToReplacements(castPairs));
		return replacements;
	}

    private String readFile(String filename) throws IOException {
		File srcFile = new File(projectOrigin, filename);
		return new String(Files.readAllBytes(srcFile.toPath()));
	}


	private static Template getTemplate(Collection<CTypeI> dataTypesToGenerate) throws IOException {
		return new Template(Collections.emptyMap(), getTypeReplacements(dataTypesToGenerate));
		
	}

	private static boolean isFloatingToWhole(Pair p) {
		return (p.a.equals(FLOAT)||p.a.equals(DOUBLE)) 
				&& (p.b.equals(BOOLEAN) || p.b.equals(SHORT) || p.b.equals(INTEGER) || p.b.equals(LONG)); 
	}
	
	private static HashMap<String, Function<String, String>> getFromToReplacements(Collection<Pair<CTypeI,CTypeI>> dataTypesToGenerate) {
		Map<String,Function<Pair<CTypeI,CTypeI>,String>> typeLookups = new HashMap<>();
		typeLookups.put("fromType", p -> p.a.getNativeJavaName());
		typeLookups.put("fromListType", p ->p.a.getListType().getNativeJavaName());
		typeLookups.put("FromType", p -> p.a.getLongJavaName());
		typeLookups.put("fromTypeChar", p -> ""+p.a.getCharacterCode());
		typeLookups.put("fromCast", p -> p.a.equals(CType.BOOLEAN) ? "? 1 : 0" : "");
		typeLookups.put("toType", p -> p.b.getNativeJavaName());
		typeLookups.put("toListType", p ->p.b.getListType().getNativeJavaName());
		typeLookups.put("ToType", p -> p.b.getLongJavaName());
		typeLookups.put("toTypeChar", p -> ""+p.b.getCharacterCode());
		typeLookups.put("toCastPre", p -> "(" + p.b.getNativeJavaName() + ")" + 
				(p.b.equals(CType.BOOLEAN) ? "(" : isFloatingToWhole(p) ? "Math.round(" : ""));
		typeLookups.put("toCastPost", p -> p.b.equals(CType.BOOLEAN) ? "== 0 ? false : true)" : 
			isFloatingToWhole(p) ? ")" : "");
		Template.TypedTemplate<Pair<CTypeI,CTypeI>> typeTemplate = new Template.TypedTemplate<>(dataTypesToGenerate, typeLookups);
		HashMap<String, Function<String,String>> replacements = new HashMap<>();
		replacements.put("FOReachPAIR", typeTemplate::apply);
		return replacements;
	}

	private static HashMap<String, Function<String, String>> getTypeReplacements(
			Collection<CTypeI> dataTypesToGenerate) {
		
		// type lookups
		Map<String,Function<CTypeI,String>> typeLookups = new HashMap<>();
		typeLookups.put("type", t ->t.getNativeJavaName());
		typeLookups.put("typeChar", t ->""+t.getCharacterCode());
		typeLookups.put("Type", t -> t.getLongJavaName());
		typeLookups.put("qname", t -> t.getQName());
		typeLookups.put("cast", t -> {int y = Math.abs(t.getTypeNum()); return y==5 ? "(short)" : y==10 ? "(char)" : y == 4 ? "(byte)" : ""; });
		typeLookups.put("toCast", t -> Math.abs(t.getTypeNum())==1 ? "== 0 ? false : true" : "");
		typeLookups.put("fromCast", t -> Math.abs(t.getTypeNum())==1 ? "? 1 : 0" : "");
		typeLookups.put("listtype", t ->t.getListType().getNativeJavaName());

		HashMap<String, Function<String,String>> replacements = new HashMap<>();

		Template.TypedTemplate<CTypeI> typeTemplate = new Template.TypedTemplate<>(dataTypesToGenerate, typeLookups);
		replacements.put("FOReachTYPE", typeTemplate::apply);

		List<CTypeI> lists = dataTypesToGenerate.stream().filter(ct -> ct.getTypeNum() >= 0).collect(Collectors.toList());
		List<CTypeI> atoms = dataTypesToGenerate.stream().filter(ct -> ct.getTypeNum() < 0).collect(Collectors.toList());

		replacements.put("FOReachLIST", new Template.TypedTemplate<>(lists, typeLookups)::apply);
		replacements.put("FOReachATOM", new Template.TypedTemplate<>(atoms, typeLookups)::apply);
		
		return replacements;
	}

	private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

}