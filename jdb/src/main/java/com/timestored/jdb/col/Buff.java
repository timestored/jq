package com.timestored.jdb.col;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Preconditions;

import lombok.ToString;

/**
 * Handles creation / closing of disk based buff'er. 
 */
@ToString
class Buff implements AutoCloseable {
	
	private MappedByteBuffer mappedByteBuffer;
	private FileChannel fileChannel;
	private RandomAccessFile randomAccessFile;
	private final File f;
	private RMode mode;
	
	public Buff(File f) { 
		this.f = Preconditions.checkNotNull(f);
	}
	
	public void close() throws IOException {
		if(mappedByteBuffer != null) {
			if(mode.equals(RMode.WRITE)) {
				mappedByteBuffer.force();
			}
			closeDirectBuffer(mappedByteBuffer);
			mappedByteBuffer = null;
		}
		if(fileChannel != null) {
			fileChannel.close();
			fileChannel = null;
		}
		if(randomAccessFile != null) {
			randomAccessFile.close();
			randomAccessFile = null;
		}
	}


	private static void closeDirectBuffer(ByteBuffer cb) {
	    if (!cb.isDirect()) return;

	    // we could use this type cast and call functions without reflection code,
	    // but static import from sun.* package is risky for non-SUN virtual machine.
	    //try { ((sun.nio.ch.DirectBuffer)cb).cleaner().clean(); } catch (Exception ex) { }
	    try {
	        Method cleaner = cb.getClass().getMethod("cleaner");
	        cleaner.setAccessible(true);
	        Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
	        clean.setAccessible(true);
	        clean.invoke(cleaner.invoke(cb));
	    } catch(Exception ex) { }
	    cb = null;
	}

	public boolean isMapped() { return mappedByteBuffer != null; }
	
	
	public void trimToSize(long sizeInBytes) throws IOException {
		close();
		randomAccessFile = new RandomAccessFile(f, RMode.WRITE.getRandomAccessFileFlag());
		randomAccessFile.setLength(sizeInBytes);
		randomAccessFile.close();
		randomAccessFile = null;
	}
	
	
	/**
	 * Create MappedByteBuffer of requested size, creating new if necessary.
	 * Where possible existing open files/channels will be used.
	 * @param sizeInBytes Size in bytes of requested buffer.
	 */
	public MappedByteBuffer map(int sizeInBytes, long offset, RMode mode) throws IOException {

		// TODO be smarter, dont close if same mode/offset etc requested again?
		if(isMapped()) {
			close();
		}
		
		this.mode = Preconditions.checkNotNull(mode); 
		if(mappedByteBuffer != null && mode.equals(RMode.WRITE)) {
			mappedByteBuffer.force();
		}
		
		if(randomAccessFile == null) {
			randomAccessFile = new RandomAccessFile(f, mode.getRandomAccessFileFlag());
		}
		if(fileChannel == null) {
			fileChannel =  randomAccessFile.getChannel();
		}
		MappedByteBuffer mbb;
		if(mappedByteBuffer == null) {
			FileChannel.MapMode mapMode = mode.equals(RMode.READ) ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
			mappedByteBuffer = fileChannel.map(mapMode, offset, sizeInBytes);
		} else {
			if(mappedByteBuffer.limit() < sizeInBytes) {
				mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, sizeInBytes);
				MappedByteBuffer old = mappedByteBuffer;
				mappedByteBuffer = mbb;
				closeDirectBuffer(old);
			}
		}
		return mappedByteBuffer;
	}
}