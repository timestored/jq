package com.timestored.jdb.server;

import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

enum MsgType { 
	ASYNC(0), SYNC(1), RESPONSE(2);
	
	private final int flag;

	MsgType(int flag) { this.flag = flag; }
	public int getFlag() { return flag; }

	private static final Map<Integer, MsgType> LOOKUP = 
			Maps.uniqueIndex(Arrays.asList(MsgType.values()), MsgType::getFlag);

	@Nullable public static MsgType fromFlag(int flag) { return LOOKUP.get(flag); }
}