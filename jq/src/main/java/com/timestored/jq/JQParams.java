package com.timestored.jq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class JQParams {

	static OptionParser getParser() {		
		
		OptionParser p = new OptionParser();
		
        p.acceptsAll(Arrays.asList("p","port"), "The TCP/IP port number to use for the SQL Server connection.")
    		.withRequiredArg().describedAs( "port_num");
        
        p.acceptsAll(Arrays.asList("q","quiet"), "Flag that if present hides all startup messages.");
        
        p.acceptsAll(Arrays.asList("P","precision"), "The number of signifiant figures to show when displaying numbers.")
			.withRequiredArg().describedAs("significant_figures").ofType(String.class);
        
        p.accepts("eval", "Evaluate the selected statement.")
			.withRequiredArg().describedAs("sql_statement").ofType(String.class);

        p.acceptsAll(Arrays.asList("c","console"), "Set the terminal console height/width.")
			.withRequiredArg().withValuesSeparatedBy(" ").describedAs("height_width").defaultsTo("20x80").ofType(String.class);
        
        p.acceptsAll(Arrays.asList("C","webconsole"), "Set the web console height/width.")
			.withRequiredArg().describedAs("height_width").defaultsTo("36x2000").ofType(String.class);

        p.accepts("prompt", "Set the prompt shown in console..")
			.withRequiredArg().describedAs("prompt").defaultsTo(")").ofType(String.class);

        p.accepts("fancyconsole", "Use a fancy console that provides completion and history search");

        p.accepts("debug", "Flag that if provides developer help on each query ran.");
        
        p.acceptsAll(Arrays.asList("?","help"), "Display a help message and exit.").forHelp();

		p.allowsUnrecognizedOptions();
		return p;
	}


	static List<String> getSystemCommands(OptionSet options) {
		List<String> cmds = new ArrayList<String>();
		// Notice this order is very specifically chosen
		for(String flag : new String[] { "c", "C", "P", "d", "p", "e" })
		if(options.has(flag)) {
			Object v = options.valueOf(flag);
			if(flag.equalsIgnoreCase("c")) {
				v = ((String) v).replace("x", " ");
			}
			cmds.add("system \"" + flag + " " + v + "\"");
		}
		return cmds;
	}
}
