package com.timestored.jq;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketImpl;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jq.ops.mono.NiladicOp;
import com.timestored.jq.ops.mono.QsOp;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

public class Context {

    private static final int MAX_HANDLES = 100_000;
	@Getter private Path currentDir;
    @Getter private JQLauncher jqLauncher;
    private final Map<Integer,OutStreamer> outStreamers = new HashMap<>();
    
    public Context(Path currentDir, JQLauncher jqLauncher) {
    	this.currentDir = Preconditions.checkNotNull(currentDir);
    	this.jqLauncher = Preconditions.checkNotNull(jqLauncher);
    	Preconditions.checkArgument(Files.isDirectory(currentDir));
	}

	public void setCurrentDir(Path path) {
		if(!Files.isDirectory(path)) {
			throw new TypeException("Can't change directory to: " + path.toString() + " as it's not a directory");
		}
		this.currentDir = path;
	}

	public void stdout(String s) { System.out.print(s); }
	public void stderr(String s) { System.err.print(s); }
	
	public int getPrecision() {
		return QsOp.INSTANCE.getPrecision();
	}
	
	public void setPrecision(int precision) {
		QsOp.INSTANCE.setPrecision(precision);
	}

	public int getConsoleRows() {
		return QsOp.INSTANCE.getRows();
	}
	
	public int getConsoleColumns() {
		return QsOp.INSTANCE.getColumns();
	}
	
	public IntegerCol getConsole() {
		return new MemoryIntegerCol(getConsoleRows(), getConsoleColumns());
	}
	
	public void setConsole(int rows, int columns) {
		QsOp.INSTANCE.setConsole(rows, columns);
	}

	public int addHandle(BufferedOutputStream bo, File f, FileDescriptor fileDescriptor) {
		return addHandle(new FileOutStreamer(bo, f, fileDescriptor));
	}


	public Object addHandle(BufferedOutputStream bo, Socket sk) {
		return addHandle(new SocketOutStreamer(bo, sk));
	}
	
	private int addHandle(OutStreamer outStreamer) {
		synchronized(outStreamers) {
			// Attempt to use the OS handle number where possible
			int fd = outStreamer.getFd();
			if(fd >= 0 && !outStreamers.containsKey(fd)) {
				outStreamers.put(fd, outStreamer);
				return fd;
			}
			fd = 3;
			for(; fd<MAX_HANDLES; fd++) {
				if(!outStreamers.containsKey(fd)) {
					outStreamers.put(fd, outStreamer);
					return fd;	
				}
			}
		}
		throw new OsException("I ran out of handle numbers");
	}
	

	
	private static long fileno(FileDescriptor fd) throws IOException {
	    try {
	        if (fd.valid()) {
	            // windows builds use long handle
	            long fileno = getFileDescriptorField(fd, "handle", false);
	            if (fileno != -1) {
	                return fileno;
	            }
	            // unix builds use int fd
	            return getFileDescriptorField(fd, "fd", true);
	        }
	    } catch (IllegalAccessException e) {
	        throw new IOException("unable to access handle/fd fields in FileDescriptor", e);
	    } catch (NoSuchFieldException e) {
	        throw new IOException("FileDescriptor in this JVM lacks handle/fd fields", e);
	    }
	    return -1;
	}

	private static long getFileDescriptorField(FileDescriptor fd, String fieldName, boolean isInt) throws NoSuchFieldException, IllegalAccessException {
	    Field field = FileDescriptor.class.getDeclaredField(fieldName);
	    field.setAccessible(true);
	    long value = isInt ? field.getInt(fd) : field.getLong(fd);
	    field.setAccessible(false);
	    return value;
	}

	
	/**
	 * @return false if the handle did not exist.
	 */
	public boolean closeHandle(int hdlNumber) {
		synchronized(outStreamers) {
			OutStreamer os = outStreamers.remove(hdlNumber);
			if(os != null) {
				try {
					os.getOS().flush();
					os.getOS().close();
				} catch (IOException e) {
					throw new OsException(e);
				}
				return true;
			} else {
				if(Database.QCOMPATIBLE) {
					return false;
				} else {
					throw new TypeException("You tried to close a non-existant handle: " + hdlNumber);
				}
			}
		}
	}

	private static interface OutStreamer {
		BufferedOutputStream getOS();
		/** The unix file number if known else negative number **/
		int getFd();
	}
	

	private static class SocketOutStreamer implements OutStreamer {
		@Getter private final BufferedOutputStream OS;
		private final Socket socket;
		@Getter private final int fd;
		
		public SocketOutStreamer(BufferedOutputStream oS, Socket socket) {
			this.OS = Objects.requireNonNull(oS);
			this.socket = Objects.requireNonNull(socket);
			this.fd = -1;
		}
		
		@Override public String toString() {
			return this.socket.getRemoteSocketAddress().toString();
		}
	}
	
	
	private static class FileOutStreamer implements OutStreamer {
		@Getter private final BufferedOutputStream OS;
		private final File f;
		@Getter private final int fd;

		public FileOutStreamer(BufferedOutputStream oS, File f, FileDescriptor fileDescriptor) {
			OS = Objects.requireNonNull(oS);
			this.f = Objects.requireNonNull(f);
			int myfd = -1;
			try {
				long l = fileno(fileDescriptor);
				if(l >= 0 && l < Integer.MAX_VALUE) {
					myfd = (int) l;
				}
			} catch (IOException e) {}
			this.fd = myfd;
		}
		
		
		@Override public String toString() { 
			return f.getAbsolutePath().toString();
		}
	}

	public Mapp getHandleDetails() {
		synchronized (outStreamers) {
			IntegerCol ic = new MemoryIntegerCol(outStreamers.keySet().stream().mapToInt(Number::intValue).toArray());
			StringCol sc = new MemoryStringCol(ic.size());
			for(int i=0; i<ic.size(); i++) {
				sc.set(i, outStreamers.get(ic.get(i)).toString());
			}
			return new MyMapp(ic, sc);
		}
	}

	public OutputStream getOutputStream(int hdlNumber) {
		synchronized (outStreamers) {
			OutStreamer os = outStreamers.get(hdlNumber);
			if(os == null) {
				throw new TypeException("No valid output found: " + hdlNumber);
			}
			return os.getOS();
		}
	}
}
