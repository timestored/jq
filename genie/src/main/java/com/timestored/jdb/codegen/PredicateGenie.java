package com.timestored.jdb.codegen;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.timestored.jdb.codegen.Genie;
import com.timestored.jdb.codegen.Template;
import com.timestored.jdb.codegen.Template.TypedTemplate;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.CTypeI;

public class PredicateGenie {
	
	private static final Genie g = new Genie("com.timestored.jdb.predicate");


	static void generateSuperPredicates() throws IOException {
		
		Map<String,Function<CTypeI,String>> typeLookups = Maps.newHashMap();
		typeLookups.put("type", t ->t.getNativeJavaName());
		typeLookups.put("Type", t -> t.getLongJavaName());
		Set<CTypeI> types = Sets.newHashSet(CType.builtinTypes());
		types.add(CType.STRING);
		types.remove(CType.BOOLEAN);
		TypedTemplate<CTypeI> typeTemplate = new Template.TypedTemplate<CTypeI>(types, typeLookups);
		

		Map<String,Function<String,String>> funcLookups = Maps.newHashMap();
		funcLookups.put("func", s -> s);
		List<String> funcs = Lists.newArrayList("equal", "greaterThanOrEqual", "greaterThan", "lessThan", "lessThanOrEqual");
		TypedTemplate<String> funcTemplate = new Template.TypedTemplate<>(funcs, funcLookups);
		
		Map<String, Function<String,String>> replacements = Maps.newHashMap();
		replacements.put("FOReachTYPE", typeTemplate::apply);
		replacements.put("FOReachFUNC", funcTemplate::apply);
		
		Template template = new Template(Collections.emptyMap(), replacements);
		
		for(String fn : new String[] {"SuperPredicates", "PredicateFactory", "PredicateFactory1", 
				"PredicateFactory2", "PredicateFactory3", "ConvertStrategy", "PF1", "PF2"}) {
			String src = Template.toString(PredicateGenie.class.getResourceAsStream("/" + fn + ".template"));
			String transformed = template.apply(src);
			g.writeFile(fn + ".java", transformed);	
		}
		
	}
	
	/**
	 * Based on the .java files for the Double versions, generate the other types by replacing strings.  
	 */
	static void generateTypeSpecificPredicates() throws IOException {

		Set<CTypeI> types = Sets.newHashSet(CType.builtinTypes());
		types.remove(CType.BOOLEAN);
		File dFile = new File(g.getPackageFile(), "DoublePredicates.java");
		g.saveTransformedDoubleFile(dFile , types);
	}


	public static void generate() throws IOException {
		generateTypeSpecificPredicates();
		generateSuperPredicates();
	}
	
	public static void main(String[] args) throws IOException {
		PredicateGenie.generate();
	}
}
