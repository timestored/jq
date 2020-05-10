package com.timestored.jq;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
//import org.jline.reader.EndOfFileException;
//import org.jline.reader.History;
//import org.jline.reader.LineReader;
//import org.jline.reader.LineReaderBuilder;
//import org.jline.reader.UserInterruptException;
//import org.jline.reader.impl.DefaultParser;
//import org.jline.reader.impl.history.DefaultHistory;
//import org.jline.terminal.Terminal;
//import org.jline.terminal.TerminalBuilder;

import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.timestored.jdb.codegen.Pair;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.server.QueryHandler;
import com.timestored.jdb.server.TsdbServer;
import com.timestored.jq.ops.OpRegister;
import com.timestored.jq.ops.mono.QsOp;

import joptsimple.HelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.Getter;
import lombok.Setter;

/** Allows running jq code and provides a REPL. */
public class JQLauncher implements QueryHandler {
	private TsdbServer server;
    private static final String PROMPT = ">";
	private static final String QQ = ".Q.host:-12!;.Q.addr:-13!;.Q.a:lower .Q.A:\"ABCDEFGHIJKLMNOPQRSTUVWXYZ\";.Q.hg:-200!;.Q.s1:-3!;.Q.s:-201!;.Q.n:\"0123456789\";";
	private final HelloRunner helloRunner;
	@Getter private final boolean debug;
	@Getter private final boolean quiet;
	@Getter private final Cache<String, ParseTree> cache = CacheBuilder.newBuilder().maximumSize(100).build();
	private final HelloLexer lexer;
	private final HelloParser parser;

    public static void main(String [] args) throws Exception {
    	
    	OptionParser cmdlParser = JQParams.getParser();
    	OptionSet options = cmdlParser.parse(args);
    	if(options.has("?") ) {
        	cmdlParser.printHelpOn(System.out);
        	System.exit(0);
    	}

    	disableAccessWarnings();
    	Logger.getLogger("io.netty").setLevel(Level.OFF);
    	Logger.getLogger("org.jline").setLevel(Level.OFF);

    	boolean quiet = options.has("q");
        if(!quiet) {
        	System.out.println("     ██╗ ██████╗ ");
        	System.out.println("     ██║██╔═══██╗");
        	System.out.println("     ██║██║   ██║");
        	System.out.println("██   ██║██║▄▄ ██║");
        	System.out.println("╚█████╔╝╚██████╔╝");
        	System.out.println(" ╚════╝  ╚══▀▀═╝ ");
        	System.out.println("                                                                    ");
        	System.out.println("Currently Supported Operations:                                     ");
        }
    	
    	JQLauncher jql = new JQLauncher(quiet, options.has("debug"), JQParams.getSystemCommands(options));
        String prompt = options.has("prompt") ? (String) options.valueOf("prompt") : quiet ? "" : PROMPT;
    	Splitter.fixedLength(80).split(OpRegister.ops.keySet().toString()).forEach(System.out::println);
    	
        if(options.has("eval")) {
        	Object o = options.valueOf("eval");
        	if(o instanceof String) {
        		jql.run((String) o);
        	}
        }
        	
        
    	if(options.has("fancyconsole")) {
//           	runTerminal(jql, prompt);
    	} else {
    		runPlainTerminal(jql, prompt);
    	}
        
        try {
        	jql.stopServer();
		} catch (InterruptedException e) {
			// TODO handle? Shutting down anyway
		}
        System.exit(0);
    }

	private static void runPlainTerminal(JQLauncher jql, String prompt) throws IOException {
       	try(Scanner scanner = new Scanner(System.in)) {
	        System.out.print(prompt);
	        while(true) {
	            String s = scanner.nextLine();
	            if(!s.isEmpty()) {
	            	String t = QsOp.INSTANCE.asText(jql.run(s));
	            	if(t.length()>0) {
	            		if(t.startsWith("'")) {
	            			System.err.print(t);
	            		} else {
	            			System.out.print(t);	            			
	            		}
	            	}
	            }
	            System.out.print(prompt);
	        }
	    }		
	}
//    
//	private static void runTerminal(JQLauncher jql, String prompt) throws IOException {
//		Terminal terminal = TerminalBuilder.builder().build();
//		DefaultParser parser = new DefaultParser();
//		parser.setEscapeChars(null);
//		parser.setQuoteChars(null);
//		LineReader reader = LineReaderBuilder.builder().terminal(terminal).parser(parser).build();
//        
//        while (true) {
//            String line = null;
//            try {
//                line = reader.readLine(prompt);
//                if(!line.isEmpty()) {
//	            	String t = QsOp.INSTANCE.asText(jql.run(line));
//	            	if(t.length()>0) {
//	            		if(t.startsWith("'")) {
//	            			System.err.print(t);
//	            		} else {
//	            			System.out.print(t);	            			
//	            		}
//	            	}
//	            }
//            } catch (UserInterruptException e) {
//                // Ignore
//            } catch (EndOfFileException e) {
//                break;
//            }
//        }
//	}

    public JQLauncher() { this(false, false, Collections.emptyList()); }

    public JQLauncher(boolean quiet, boolean debug, List<String> systemCommands) {
		helloRunner = new HelloRunner(new Context(Paths.get(""), this));
		this.quiet = quiet;
		this.debug = debug; //args.length > 0 && "-debug".equalsIgnoreCase(args[0]));

		CharStream cs = CharStreams.fromString("\n");
        lexer = new HelloLexer(cs);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        parser = new HelloParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        
    	Object res = run(QQ);
    	if(res instanceof RuntimeException) {
    		System.exit(-1);
    	}
        for(String sysCmd : systemCommands) {
        	res = run(sysCmd);
        	if(res instanceof RuntimeException) {
        		System.exit(-1);
        	}
        }
	}
    


	public void startPort(int port) throws InterruptedException {
		if(server != null && port == server.getPort()) {
			// do nothing as already running
		} else {
			stopServer();
			if(port != 0) {
				server = new TsdbServer(this, port);
			}
		}
	}
	
	void stopServer() throws InterruptedException {
		if(server != null) {
			server.shutdown();
		}
	}
	
	
    
    public Object run(String s) {
    	if(s.trim().isEmpty()) {
    		return null;
    	}

        try {
        	ParseTree res = cache.get(s, new Callable<ParseTree>() {
        		@Override public ParseTree call() throws Exception {
        			CharStream cs = CharStreams.fromString("\n" + s);
        			lexer.reset();
        			parser.reset();
        			lexer.setInputStream(cs);
        			parser.setInputStream(new CommonTokenStream(lexer));
                    return parser.r();
        		}

			});
            

        	if(debug) {
                List<String> ruleNamesList = Arrays.asList(lexer.getRuleNames());
                String prettyTree = TreeUtils.toPrettyTree(res, ruleNamesList);
                System.out.println(prettyTree);
        	}

            return helloRunner.visit(res);
        } catch (RuntimeException e) {
        	if(debug) {
        		System.err.println(e.toString());
        		e.printStackTrace();
        	}
            return e;
        } catch (ExecutionException e) {
        	if(debug) {
        		System.err.println(e.toString());
        		e.printStackTrace();
        	}
            return e.getCause();
		}
    }

    public void display(String s) { System.out.println(s); }
    
    private static class ThrowingErrorListener extends BaseErrorListener {
        public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
                throws ParseCancellationException {
            throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
        }
    }

	@Override public Object query(Object o) {
		String s = getString(o);
		if(s != null) {
			return run(s);
		}
		return null;
	}
	

	/**
	 * Check if the data is a string (list of characters) and if so 
	 * @return A string or null if not possible.
	 */
	private String getString(Object o) {
		String s = null;
		if(o instanceof CharacterCol) {
			CharacterCol charCol = (CharacterCol) o;
			StringBuilder sb = new StringBuilder(charCol.size());
			for(int i=0; i<charCol.size(); i++) {
				sb.append(charCol.get(i));
			}
			s = sb.toString();
		} else if(o instanceof String) {
			s = (String) o;
		} else if(o instanceof Character) {
			s = String.valueOf((char) o);
		}
		return s;
	}
	
	  @SuppressWarnings("unchecked")
	    public static void disableAccessWarnings() {
	        try {
	            Class unsafeClass = Class.forName("sun.misc.Unsafe");
	            Field field = unsafeClass.getDeclaredField("theUnsafe");
	            field.setAccessible(true);
	            Object unsafe = field.get(null);

	            Method putObjectVolatile = unsafeClass.getDeclaredMethod("putObjectVolatile", Object.class, long.class, Object.class);
	            Method staticFieldOffset = unsafeClass.getDeclaredMethod("staticFieldOffset", Field.class);

	            Class loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger");
	            Field loggerField = loggerClass.getDeclaredField("logger");
	            Long offset = (Long) staticFieldOffset.invoke(unsafe, loggerField);
	            putObjectVolatile.invoke(unsafe, loggerClass, offset, null);
	        } catch (Exception ignored) {
	        }
	    }

	public int getCurrentPort() { return server!=null ? server.getPort() : 0; }
}