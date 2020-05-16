package com.timestored.jdb.database;

import java.io.DataInput;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ToFromIntDate;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.col.MemoryByteCol;
import com.timestored.jdb.col.MemoryCharacterCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryDoubleCol;
import com.timestored.jdb.col.MemoryFloatCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jdb.col.MemoryShortCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.RMode;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.StringMap;
import com.timestored.jdb.col.ToFromLongTimestamp;
import com.timestored.jdb.iterator.Locations;

/**
 * Reads a standardised IPC format and returns the Cols/Objects that stream represented. 
 */
public class IpcDataReader {

	private final DataInput in;
	
	private IpcDataReader(DataInput in) { 
		this.in = Preconditions.checkNotNull(in); 
	}

	public static Object read(DataInput in) throws IOException {
		return new IpcDataReader(in).load();
	}
	
	/*
	 * We don't handle chars/char lists so there are 4 possible incoming values:
	 * 1. "asdasdasd" char array = String
	 * 2. "a" single char = String
	 * 3. ("nested";"chars") = StringCol
	 * 4. `symbol = String
	 * 5. `nested`symbols = StringCol
	 */

	private Object load() throws IOException {
		byte typeNum = in.readByte();
		
		if(typeNum==99) {
			Col k = (Col) load();
			Col v = (Col) load();
			return new MyMapp(k,v); 
		} else if(typeNum==98) {
			in.readByte(); // ??
			return new MyTbl((Mapp) load());
		} else if(typeNum > 0) {
			return loadList(typeNum);
		}

		switch (typeNum) {
			case 0:
				in.readByte();
				int sz = (int) in.readInt();
				ObjectCol ml = new MemoryObjectCol(sz);
				ml.setSize(sz);
				for(int i=0; i<sz; i++) {
					ml.set(i, load());
				}
				return ml;
			case -1: return in.readBoolean();
			case -4: return in.readByte();
			case -5: return in.readShort();
			case -6: return in.readInt();
			case -7: return in.readLong();
			case -8: return in.readFloat();
			case -9: return in.readDouble();
			case -10: return (char)in.readByte();
			case -11: return readString(in);
			case -12: return ToFromLongTimestamp.INSTANCE.apply(in.readLong());
			case -14: return ToFromIntDate.INSTANCE.apply(in.readInt());
		}
		

		if(typeNum == 98) { // table
			in.readByte(); // attributes
			Preconditions.checkArgument(in.readByte() == 99); // type = dict
			StringCol mc = (StringCol) load();
//			IntegerCol positions = colNames.find(mc);
//			if(positions.contains(colNames.size())) {
//				throw new RuntimeException("Column not found");
//			}
		}
		
		
		return null;
	}


	private Object loadList(byte typeNum) throws IOException {

		in.readByte();
		int sz = (int) in.readInt();
		
		switch (typeNum) {
			case 1:	{
				BooleanCol c = new MemoryBooleanCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readBoolean()); } 
				return c; }
			case 4:	{
				ByteCol c = new MemoryByteCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readByte()); } 
				return c; }
			case 5: {
				ShortCol c = new MemoryShortCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readShort()); }
				return c; }
			case 6: {
				IntegerCol c = new MemoryIntegerCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readInt()); }
				return c; }
			case 7:  {
				LongCol c = new MemoryLongCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readLong()); }
				return c; }
			case 8:  {
				FloatCol c = new MemoryFloatCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readFloat()); }
				return c; }
			case 9:  {
				DoubleCol c = new MemoryDoubleCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, in.readDouble()); }
				return c; }
			case 10: {
				CharacterCol c = new MemoryCharacterCol(sz);
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, (char) in.readByte()); }
				return c; }
			case 11: {
				MemoryStringCol c = new MemoryStringCol(sz);	
				l(sz, c);
				for(int i=0; i<sz; i++) { c.set(i, readString(in)); }
				return c; }
			default: {
				throw new UnsupportedOperationException("List type not supported:" + typeNum);
			}
		}

	}


	private static void l(int sz, Col v) throws IOException {
		v.setSize(sz);
		v.map(Locations.upTo(sz), RMode.WRITE);
	}



	private static String readString(DataInput in) throws IOException {
		byte b = in.readByte();
		StringBuilder sbb = new StringBuilder();
		while(b != 0) {
			sbb.append((char) b);
			b = in.readByte();
		}
		return sbb.toString();
	}
	
	

}
