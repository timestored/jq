package com.timestored.jdb.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.server.QueryHandler;
import com.timestored.jdb.server.TsdbServer;

import lombok.extern.java.Log;

@Log
public class Database implements QueryHandler {

	private static final int PORT = 5000;
	public static final boolean QCOMPATIBLE = true;
	private final File folder;
	private TsdbServer server;
	
	public Database(File folder) throws IOException {
		
		this.folder = Preconditions.checkNotNull(folder);
		Preconditions.checkArgument(folder.isDirectory() && folder.canRead() && folder.exists());
		System.out.println("Starting DB in folder: " + folder.getAbsolutePath());
		
		File stringMapFile = new File(folder, "stringmap.sym");
		
		if(stringMapFile.exists()) {
			ObjectInputStream in;
			try {
				in = new ObjectInputStream(new FileInputStream(stringMapFile));
				in.readObject();
			} catch (IOException | ClassNotFoundException e) {
				System.err.println("stringmap.sym found but couldn't read.");
			}
		}
		
		try {
			startPort(PORT);
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "Could not bind to port " + PORT, e);
		}
	}
	
	void startPort(int port) throws InterruptedException {
		stopServer();
		server = new TsdbServer(this, PORT);
	}
	
	void stopServer() throws InterruptedException {
		if(server != null) {
			server.shutdown();
		}
	}
	
	void shutdown() {
		try {
			stopServer();
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "Error closing port: " + PORT, e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		File d = Files.createTempDir();
		System.out.println(d.getAbsolutePath());
		Database db = new Database(d);
	}
	

	@Override public Object query(Object o) {
		String s = getString(o);
		return "err657";
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
}