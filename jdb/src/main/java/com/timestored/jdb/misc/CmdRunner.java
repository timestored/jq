package com.timestored.jdb.misc;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.timestored.jdb.kexception.OsException;

/**
 * Allows running commands as if at the terminal. 
 */
public class CmdRunner {

	private static final Logger LOG = Logger.getLogger(CmdRunner.class.getName());

	/**
	 * @param command a specified system command
	 * @param envp array of strings, each element of which has environment variable settings in the format name=value, or null if the subprocess should inherit the environment of the current process. 
	 * @param dir the working directory of the subprocess, or null if the subprocess should inherit the working directory of the current process.
	 * @return The result of running the command.
	 */
	public static String run(String commands[], String[] envp, File dir) throws IOException {
		Process p = Runtime.getRuntime().exec(commands, envp, dir);
		LOG.info("getRuntime().exec " + commands);
		return waitGobbleReturn(p);
	}

	private static String waitGobbleReturn(Process p) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		new Thread(new StreamGobbler(p.getInputStream(), ps)).start();
		new Thread(new StreamGobbler(p.getErrorStream(), ps)).start();
		try {
			p.waitFor();
			return baos.toString("utf-8");
		} catch (InterruptedException e) { 
			LOG.log(Level.SEVERE, "CmdRunner Run Error", e);
		} catch(UnsupportedEncodingException e) {
			LOG.log(Level.SEVERE, "CmdRunner Run Error", e);
		}
		return "";
	}
	

	/**
	 * @param dir 
	 * @param Terminal command.
	 * @return The result of running the command.
	 * @throws RuntimeException If the exit value was anything other than 0.
	 */
	public static String runAsScript(String command, File dir) throws IOException {
		boolean isWindows = System.getProperty("os.name").toLowerCase().contains("windows");
		Path shFile = Files.createTempFile("cmdrun", isWindows ? ".bat" : ".sh");
		Files.write(shFile, ("@echo off\r\n" + command).getBytes());
		String pth = shFile.toAbsolutePath().toString();
	    String result = "";
	    ProcessBuilder builder = null;
	    
		if(isWindows) {
			builder = new ProcessBuilder(new String[]{"cmd.exe", "/c", pth});
		} else {
			builder = new ProcessBuilder(new String[] {"chmod u+x " + pth + "; " + pth});
		}
		builder.directory(dir.getAbsoluteFile());
		Process p = builder.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
		StringJoiner sj = new StringJoiner(System.getProperty("line.separator"));
        reader.lines().iterator().forEachRemaining(sj::add);
        result = sj.toString();
        try {
			p.waitFor();
		} catch (InterruptedException e) {
			throw new OsException(e.toString());
		}
        p.destroy();
		if(p.exitValue() != 0) {
			throw new OsException(result);
		}
		return result;
	}
	
	/**
	 * @param Terminal commands where each is a separate string in an array.
	 * @return The result of running the command.
	 */
	public static String run(String[] commands) throws IOException { return run(commands, null, null); }

	/**
	 * @param Terminal command.
	 * @return The result of running the command.
	 */
	public static String run(String command) throws IOException { 
		Process p = Runtime.getRuntime().exec(command, null, null);
		LOG.info("getRuntime().exec " + command);
		return waitGobbleReturn(p);
	}

	/** 
	 * @param command a specified system command
	 * @param dir the working directory of the subprocess, or null if the subprocess should inherit the working directory of the current process.
	 * @return The result of running the command.
	 */
	public static Process startProc(String command, File dir) throws IOException {
		Process p = Runtime.getRuntime().exec(command, null, dir);
		gobbleStreams(p);
		return p;
	}
	
	/**
	 * @param commands a specified system command
	 * @param envp array of strings, each element of which has environment variable settings in the format name=value, or null if the subprocess should inherit the environment of the current process. 
	 * @param dir the working directory of the subprocess, or null if the subprocess should inherit the working directory of the current process.
	 */
	public static Process startProc(String[] commands, String[] envp, File dir) throws IOException {
		Process p = Runtime.getRuntime().exec(commands, envp, dir);
		LOG.info("getRuntime().exec " + Joiner.on(' ').join(commands));
		gobbleStreams(p);
		return p;
	}

	private static void gobbleStreams(Process p) {
		new Thread(new StreamGobbler(p.getInputStream(), System.out)).start();
		new Thread(new StreamGobbler(p.getErrorStream(), System.err)).start();
	}


	/** Consumes streams to ensure that process completes. */
	private static class StreamGobbler implements Runnable {
	
		private InputStreamReader isr;
		private PrintStream ps;
		
		public StreamGobbler(InputStream is, PrintStream outputPS) {
	        isr = new InputStreamReader(is);
	        this.ps = Preconditions.checkNotNull(outputPS);
		}
		
		@Override public void run() {
	        try {
	        	BufferedReader br = new BufferedReader(isr);
	            String line=null;
	            while ( (line = br.readLine()) != null) {
					ps.println(line);
				}    
	        } catch (IOException ioe) {
	            ioe.printStackTrace();  
	        }
	    }
	}

	private static final String REG_EXP = "\"(\\\"|[^\"])*?\"|[^ ]+";
	private static final Pattern PATTERN = Pattern.compile(REG_EXP, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

	/** 
	 * Convert string to separate arguments similar to what the OS usually does
	 * i.e. careful of quotes. 
	 **/
	public static String[] parseCommand(String cmd) {
		if (cmd == null || cmd.length() == 0) {
			return new String[] {};
		}

		cmd = cmd.trim();
		Matcher matcher = PATTERN.matcher(cmd);
		List<String> matches = new ArrayList<String>();
		while (matcher.find()) {
			String s = matcher.group();
			if(s.length()>=2) {
				boolean hasQuotes = (s.charAt(0)=='"' && s.charAt(s.length()-1)=='"')
						|| (s.charAt(0)=='\'' && s.charAt(s.length()-1)=='\'');
				if(hasQuotes) {
					s = s.substring(1, s.length()-1);
				}
			}
			matches.add(s);
		}
		String[] parsedCommand = matches.toArray(new String[] {});
		return parsedCommand;
	}
}
