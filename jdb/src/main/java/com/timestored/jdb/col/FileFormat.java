package com.timestored.jdb.col;

import java.util.Arrays;
import java.util.Map;

import lombok.Getter;

import com.google.common.collect.Maps;

enum FileFormat { 
	FLAT_FORMAT((byte)0x00), COMPRESSED((byte)0x01);

	private static final Map<Byte, FileFormat> LOOKUP = Maps.uniqueIndex(
            Arrays.asList(FileFormat.values()),
            FileFormat::getHtag);
	
	@Getter private byte htag;
	
	private FileFormat(byte htag) { this.htag = htag; }
	public static FileFormat fromHtag(byte htag) { return LOOKUP.get(htag); }
}