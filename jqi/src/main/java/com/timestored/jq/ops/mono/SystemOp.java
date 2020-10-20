package com.timestored.jq.ops.mono;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jdb.misc.CmdRunner;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class SystemOp extends BaseMonad {
	public static SystemOp INSTANCE = new SystemOp();
	@Override public String name() { return "system"; }

    public Object run(Object o) {
    	String s = null;
    	if(o instanceof CharacterCol) {
    		s = (String) CastOp.CAST.s((CharacterCol) o);
    	} else if(o instanceof Character) {
    		s = "" + (char) o;
	    } else if(o instanceof String) {
	    	s = (String)o;
	    } else if(Database.QCOMPATIBLE && TypeOp.isNotList(o)) {
			return o;
		}
	    if(s==null || s.trim().length()==0) {
			return ColProvider.emptyCol(CType.CHARACTER);
		} 
    	
		String[] cmds = s.split(" ");
		
		if(cmds.length == 1) {
			// Printing current status
			switch(cmds[0]) {
				case "p": return context.getJqLauncher().getCurrentPort();
				case "P": return context.getPrecision();
				case "c": return context.getConsole();
				case "a": return getVariables("", obj -> obj instanceof Tbl);
				case "f":  return getVariables("", obj -> TypeOp.TYPE.type(o) == 100);
				case "\\": System.exit(0);
				case "v": return getVariables("", obj -> true);
				case "l": throw new NYIException();
				case "cd": return cd(cmds);
			}
		}
		// Setters, linux commands and others.
		if(s.contains(" ")) {
			Object r = tryBuiltinCommand(s);
			if(r != null) {
				return r;
			}
		}
		// run windows/linux commands
		try {
			String[] res = CmdRunner.runAsScript(s, context.getCurrentDir().toFile()).replace("\r", "").split("\n");
			return ColProvider.toCharacterCol(res);
		} catch (IOException e) {
			return new RuntimeException(e);
		}
    }

    private StringCol getVariables(String ns, Predicate<Object> filter) {
    	Mapp m = frame.getMapp(ns);
    	StringCol sc = (StringCol) m.getKey();
    	List<String> matches = new ArrayList<>(); 
    	for(int i=0; i<sc.size(); i++) {
    		String name = sc.get(i);
    		if(filter.test(frame.get(name))) {
    			matches.add(name);
    		}
    	}
    	return ColProvider.toStringCol(matches);
    }
    
    /**
     * Run a builtin command if it exists else return null.  
     * Nothing else must ever return null.
     */
	private Object tryBuiltinCommand(String s) {
		String[] cmds = s.split(" ");
		Preconditions.checkArgument(cmds.length >= 2);
		String c = cmds[0];
		switch(c) {
		case "cd":  return cd(cmds);
		case "a":  return getVariables(cmds[1], obj -> obj instanceof Tbl);
		case "f":  return getVariables(cmds[1], obj -> TypeOp.TYPE.type(obj) == 100);
		case "v":  return getVariables(cmds[1], obj -> true);
		case "\\": System.exit(0);
		case "l": return loadFile(cmds[1]);
		case "p":
			try {
				int port = Integer.parseInt(cmds[1]);
				context.getJqLauncher().startPort(port);
			} catch (NumberFormatException | InterruptedException e) {
				return new DomainException("Cannnot start server listening on port " + cmds[1]);
			}
			return NiladicOp.INSTANCE;
		case "P": 
			context.setPrecision(Integer.parseInt(cmds[1]));
			return NiladicOp.INSTANCE;
		case "c": 
			try {
				if(cmds.length != 3) {
					throw new DomainException("Console arguments wrong size.");
				}
			context.setConsole(Integer.parseInt(cmds[1]), Integer.parseInt(cmds[2]));
			} catch(NumberFormatException nfe) {
				throw new DomainException("Couldn't understand console size: " + Arrays.deepToString(cmds));
			}
			return NiladicOp.INSTANCE;
		}
		return null;
	}

	private Object loadFile(String filePath) {
		File f = new File(filePath);
		if(!f.exists()) {
			throw new TypeException("File to be loaded doesn't exist");
		}
		String code;
		try {
			code = Files.toString(f, Charset.defaultCharset());
		} catch (IOException e) {
			throw new TypeException("Problem reading file: " + f.getAbsolutePath() + " " + e.toString());
		}
		Object res = context.getJqLauncher().run(code);
		return res == null ? NiladicOp.INSTANCE : res; 
	}

	private Object cd(String[] cmds) {
		if(cmds.length == 1) {
			return StringOp.INSTANCE.run(this.context.getCurrentDir().toAbsolutePath().toString());
		} else if(cmds.length == 2) {
			this.context.setCurrentDir(Paths.get(cmds[1]));
			return NiladicOp.INSTANCE;
		}
		throw new IllegalArgumentException("invalid number of commands to cd");
	}
}
