package com.timestored.jdb.database;

import com.google.common.io.BaseEncoding;

public class JUtils {

	private static final BaseEncoding HEX_DECODER = BaseEncoding.base16().lowerCase();
	
	public static final String toString(byte[] data) {
		StringBuilder sb = new StringBuilder();
		for (int j=0; j<data.length; j++) {
		   sb.append(String.format("%02X ", data[j]));
		}
		return sb.toString();
	}

	public static final byte[] decode(String hexString) {
		String s = hexString.replace(" ", "").toLowerCase();
		s = 1 == s.length() % 2 ? "0"+s : s;
		return HEX_DECODER.decode(s);
	}
}
