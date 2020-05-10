package com.timestored.jdb.col;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import lombok.Getter;
import lombok.ToString;

/**
 *  Each Buff has a header that defines its type and head size (for future expansion purposes)
 */
@ToString
public class BuffHeader {
	
	/*
	 * A 4 byte header of the format:
	 * headerSize, fileFormat, 0, 0
	 */
	public static final byte DEFAULT_HEADER_SIZE = 0x04;
	
	@Getter private final FileFormat fileFormat;
	@Getter private final byte headerSize;
	private final File file;
	private long fileSize;
	
	/**
	 * Given the byte width of the type of data that will be stored
	 * Return the number of items that there are in the file.
	 */
	public int getNumberOfItems(int byteWidth) {
		return (int) ((fileSize - headerSize)/byteWidth);
	}
	
	public BuffHeader(File file, FileFormat fileFormat) { this(file, fileFormat, DEFAULT_HEADER_SIZE); }
	
	private BuffHeader(File file, FileFormat fileFormat, byte headerSize) {
		this.fileFormat = fileFormat;
		this.headerSize = headerSize;
		this.file = file;
		this.fileSize = file.length();
	}
	
	byte[] getBytes() { return new byte[] { headerSize, fileFormat.getHtag(), 0, 0 }; }

	static BuffHeader read(File file) throws FileNotFoundException, IOException {
		if(file.exists()) {
			byte[] buffer = new byte[4];
			try(InputStream is = new FileInputStream(file)) {
				if (is.read(buffer) != buffer.length) {
					throw new IllegalArgumentException("cant open file: " + file);
				} else {
				    byte headerSize = buffer[0];
			    	FileFormat fileFormat = FileFormat.fromHtag(buffer[1]);
			    	if(fileFormat == null) {
						throw new IllegalArgumentException("Illegal format. " + fileFormat + " cant open file: " + file);
			    	}
			    	return new BuffHeader(file, fileFormat, headerSize);
				}
			}
		}
		throw new IllegalArgumentException(file + " doesnt exist.");
	}
	

}