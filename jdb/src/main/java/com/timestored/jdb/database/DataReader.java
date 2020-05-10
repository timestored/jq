package com.timestored.jdb.database;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;

import com.timestored.jdb.col.*;

public interface DataReader {

	void visit(BooleanCol booleanCol) throws IOException;
	void visit(ByteCol byteCol) throws IOException;
	void visit(ShortCol shortCol) throws IOException;
	void visit(CharacterCol charCol) throws IOException;
	void visit(IntegerCol integerCol) throws IOException;
	void visit(LongCol longCol) throws IOException;
	void visit(DoubleCol doubleCol) throws IOException;
	void visit(FloatCol floatCol) throws IOException;
	void visit(StringCol stringCol) throws IOException;
	void visit(DateCol dateCol) throws IOException;
	void visit(ObjectCol mixedList) throws IOException;

	void visit(boolean booleanVal) throws IOException;
	void visit(byte byteVal) throws IOException;
	void visit(short shortVal) throws IOException;
	void visit(char charVal) throws IOException;
	void visit(int integerVal) throws IOException;
	void visit(long longVal) throws IOException;
	void visit(double doubleVal) throws IOException;
	void visit(float floatVal) throws IOException;
	void visit(String stringVal) throws IOException;
	void visit(Date date) throws IOException;
	void visit(Exception e) throws IOException;

	void visitUnknown(Object o) throws IOException;

	default void visit(Object o) throws IOException {
		if(o instanceof BooleanCol) {
			visit((BooleanCol) o);
		} else if(o instanceof ByteCol) {
			visit((ByteCol) o);
		} else if(o instanceof ShortCol) {
			visit((ShortCol) o);
		} else if(o instanceof CharacterCol) {
			visit((CharacterCol) o);
		} else if(o instanceof IntegerCol) {
			visit((IntegerCol) o);
		} else if(o instanceof LongCol) {
			visit((LongCol) o);
		} else if(o instanceof DoubleCol) {
			visit((DoubleCol) o);
		} else if(o instanceof FloatCol) {
			visit((FloatCol) o);
		} else if(o instanceof StringCol) {
			visit((StringCol) o);
		} else if(o instanceof TimstampCol) {
			visit((TimstampCol) o);
		} else if(o instanceof DateCol) {
			visit((DateCol) o);
		} else if(o instanceof ObjectCol) {
			visit((ObjectCol) o);
		} else if(o instanceof Boolean) {
			visit((boolean)(Boolean) o);
		} else if(o instanceof Byte) {
			visit((byte)(Byte) o);
		} else if(o instanceof Short) {
			visit((short)(Short) o);
		} else if(o instanceof Character) {
			visit((char) (Character) o);
		} else if(o instanceof Integer) {
			visit((int)(Integer) o);
		} else if(o instanceof Long) {
			visit((long)(Long) o);
		} else if(o instanceof Double) {
			visit((double)(Double) o);
		} else if(o instanceof Float) {
			visit((float)(Float) o);
		} else if(o instanceof String) {
			visit((String) o);
		} else if(o instanceof Exception) {
			visit((Exception) o);
		} else if(o instanceof Tbl) {
			visit((Tbl) o);
		} else if(o instanceof Mapp) {
			visit((Mapp) o);
		} else if(o instanceof ResultSet) {
			visit((ResultSet) o);
		} else if(o instanceof Object[]) {
			visit((Object[]) o);
		} else {
			visitUnknown(o);
		}
	}
	void visit(ResultSet rs) throws IOException;
	void visit(Mapp mapp) throws IOException;
	void visit(Tbl tbl) throws IOException;
	void visit(Object[] objectArray) throws IOException;

	
}
