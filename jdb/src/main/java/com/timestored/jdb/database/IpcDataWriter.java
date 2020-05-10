package com.timestored.jdb.database;

import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.timestored.jdb.col.*;
import com.timestored.jdb.iterator.*;
import com.timestored.jdb.resultset.KeyedResultSet;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class IpcDataWriter implements DataReader {
	
	private final DataOutput stream;

	private void writeHead(int size, int typ) throws IOException {
		stream.writeByte(typ);
	    stream.writeByte(0);
    	stream.writeInt(size);
	}
	

	public void visit(char[] charVal) throws IOException {	
		writeHead(charVal.length, 10);
		for(char c: charVal) {
			stream.write((int) c);
		}
	}

	@Override public void visit(boolean booleanVal) throws IOException {	
		stream.writeByte(-1); stream.writeBoolean(booleanVal);	}
	@Override public void visit(byte byteVal) throws IOException {	
		stream.writeByte(-4); stream.writeByte(byteVal);	}
	@Override public void visit(short shortVal) throws IOException {
		stream.writeByte(-5); stream.writeShort(shortVal);	}
	@Override public void visit(char charVal) throws IOException {
		stream.writeByte(-10); stream.writeByte(charVal);	}
	@Override public void visit(int integerVal) throws IOException {
		stream.writeByte(-6); stream.writeInt(integerVal);	}
	@Override public void visit(long longVal) throws IOException {
		stream.writeByte(-7); stream.writeLong(longVal);	}
	@Override public void visit(float floatVal) throws IOException {
		stream.writeByte(-8); stream.writeFloat(floatVal);	}
	@Override public void visit(double doubleVal) throws IOException {
		stream.writeByte(-9); stream.writeDouble(doubleVal);	}
	@Override public void visit(Date date) throws IOException {
		stream.writeByte(CType.DATE.getTypeNum()); stream.writeLong(ToFromIntDate.INSTANCE.applyAsInt(date));
	}

	@Override public void visit(Mapp mapp) throws IOException {
		stream.writeByte(99);
		visit(mapp.getKey());
		visit(mapp.getValue());
	}

	@Override public void visit(Tbl tbl) throws IOException {
		stream.writeByte(98);
		stream.writeByte(0);
		visit((Mapp) tbl);
	}

	private void visitWithin(Object o) throws IOException {
		if(o instanceof Boolean) {
			visitWithin((boolean) (Boolean) o);
		} else if(o instanceof Byte) {
				visitWithin((byte)(Byte) o);
		} else if(o instanceof Short) {
			visitWithin((short)(Short) o);
		} else if(o instanceof Character) {
			visitWithin((char)(Character) o);
		} else if(o instanceof Integer) {
			visitWithin((int)(Integer) o);
		} else if(o instanceof Long) {
			visitWithin((long)(Long) o);
		} else if(o instanceof Double) {
			visitWithin((double)(Double) o);
		} else if(o instanceof Float) {
			visitWithin((float)(Float) o);
		} else if(o instanceof String) {
			visitWithin((String) o);
		} else if(o instanceof Timestamp) {
			visitWithin((Timestamp) o);
		} else if(o instanceof Date) {
			visitWithin((Date) o);
		} else {
			throw new UnsupportedOperationException("visitWIthin failed to find type");
		}
	}

	private void visitWithin(boolean booleanVal) throws IOException {	
		stream.writeBoolean(booleanVal);	}
	private void visitWithin(byte byteVal) throws IOException {	
		stream.writeByte(byteVal);	}
	private void visitWithin(short shortVal) throws IOException {
		stream.writeShort(shortVal);	}
	private void visitWithin(char charVal) throws IOException {
		stream.writeByte(charVal);	}
	private void visitWithin(int integerVal) throws IOException {
		stream.writeInt(integerVal);	}
	private void visitWithin(long longVal) throws IOException {
		stream.writeLong(longVal);	}
	private void visitWithin(float floatVal) throws IOException {
		stream.writeFloat(floatVal);	}
	private void visitWithin(double doubleVal) throws IOException {
		stream.writeDouble(doubleVal);	}
	private void visitWithin(String stringVal) throws IOException {
		stream.write(stringVal.getBytes()); stream.writeByte(0); }
	private void visitWithin(Timestamp timestamp) throws IOException {
		stream.writeLong(ToFromLongTimestamp.INSTANCE.applyAsLong(timestamp));
	}
	private void visitWithin(Date date) throws IOException {
		stream.writeInt(ToFromIntDate.INSTANCE.applyAsInt(date));
	}

	private void visit(KeyedResultSet krs) throws IOException {
		try {
			int nk = krs.getNumberOfKeyColumns();
			if(nk>0) {
				ResultSetMetaData md = krs.getMetaData();
			    int colCount = md.getColumnCount();
			    int[] keyCols = new int[nk];
			    int[] nonKeyCols = new int[colCount - nk];
			    int i = 0;
			    while(i < nk) {
			    	keyCols[i] = ++i;
			    }
			    while(i < colCount) {
			    	nonKeyCols[i-nk] = ++i;
			    }
				stream.writeByte(99);
				visit(krs, keyCols);
				visit(krs, nonKeyCols);
			} else {
				visit((ResultSet) krs);
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private void visit(ResultSet rs, int[] colIdxs) throws IOException {
		try {
			stream.writeByte(98);
			stream.writeByte(0); // attributes
			stream.writeByte(99);
			ResultSetMetaData md = rs.getMetaData();
			
			// Write the column names
			stream.writeByte(-CType.STRING.getTypeNum());
		    stream.writeByte(0);
		    int colCount = colIdxs.length;
	    	stream.writeInt(colCount);
			for(int c = 0; c<colCount; c++) {
				visitWithin(md.getColumnName(colIdxs[c]));
			}
	
			// mixed list
			stream.writeByte(0); // type
			stream.writeByte(0); // attributes
			stream.writeInt(colCount);
	
			rs.last();
		    int size = rs.getRow();
			for(int c = 0; c<colCount; c++) {
				CType cType = CType.fromSqlType(md.getColumnType(colIdxs[c]));
				if(cType.getTypeNum() == 0) {
					throw new RuntimeException("RS column type unrecognised");
				}
			    rs.beforeFirst();
				if(cType.equals(CType.STRING)) {
					writeHead(size, 0);
					while (rs.next()) {
					    visit(((String)rs.getObject(colIdxs[c])).toCharArray());
					}
				} else {
					writeHead(size, cType.getTypeNum());
					while (rs.next()) {
					    visitWithin(rs.getObject(colIdxs[c]));
					}
				}
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
	@Override public void visit(ResultSet rs) throws IOException {
		if(rs instanceof com.timestored.jdb.resultset.KeyedResultSet && ((KeyedResultSet)rs).getNumberOfKeyColumns()>0) {
			visit((KeyedResultSet) rs);
		} else {
			try {
				ResultSetMetaData md = rs.getMetaData();
			    int colCount = md.getColumnCount();
			    int[] cols = new int[colCount];
			    for(int i=0; i<colCount; i++) {
			    	cols[i] = i+1;
			    }
				visit(rs, cols);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
	}
	
	
	@Override public void visit(String stringVal) throws IOException {
		stream.writeByte(-11);
		byte[] bytes = stringVal.getBytes();
		stream.write(bytes);
		stream.writeByte(0);
	}
	
	@Override public void visit(Exception e) throws IOException {
		stream.writeByte(-128);
		stream.write(e.toString().getBytes());
		stream.writeByte(0);
	}

	
	@Override public void visitUnknown(Object o) throws IOException {
//		KeyedResultSet rs = ResultSetAdapter.get(o);
//		if(rs != null) {
//			visit(rs);
//		} else {
			// empty projection
			stream.writeByte(101);
			stream.writeByte(00);
//		}
	}

	@Override public void visit(StringCol c) throws IOException {
		writeHead(c.size(), c.getType());
		StringIter it = c.select();
		while(it.hasNext()) {
		String s = it.nextString();
			if(s != null) {
				stream.write(s.getBytes());
			}
			stream.writeByte(0);
		}
	}
	
	@Override public void visit(BooleanCol c) throws IOException {
		writeHead(c.size(), c.getType());
    	BooleanIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeBoolean(it.nextBoolean());
    	}
	}
	
	@Override public void visit(ByteCol c) throws IOException {
		writeHead(c.size(), c.getType());
    	ByteIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeByte(it.nextByte());
    	}
	}

	@Override public void visit(DoubleCol c) throws IOException {
		writeHead(c.size(), c.getType());
    	DoubleIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeDouble(it.nextDouble());
    	}	
	}

	@Override public void visit(ShortCol c) throws IOException {
		writeHead(c.size(), c.getType());
		ShortIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeShort(it.nextShort());
    	}	
	}

	@Override public void visit(CharacterCol c) throws IOException {
		writeHead(c.size(), c.getType());
		CharacterIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeByte(it.nextCharacter());
    	}	
	}

	@Override public void visit(LongCol c) throws IOException {
		writeHead(c.size(), c.getType());
		LongIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeLong(it.nextLong());
    	}	
	}

	@Override public void visit(IntegerCol c) throws IOException {
		writeHead(c.size(), c.getType());
		IntegerIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeInt(it.nextInteger());
    	}	
	}

	@Override public void visit(FloatCol c) throws IOException {
		writeHead(c.size(), c.getType());
		FloatIter it = c.select();
    	while(it.hasNext()) {
    		stream.writeFloat(it.nextFloat());
    	}	
	}

	@Override public void visit(DateCol c) throws IOException {
		writeHead(c.size(), c.getType());
		if(c instanceof IntegerBackedDateCol) {
			IntegerIter it = ((IntegerBackedDateCol)c).getC().select();
	    	while(it.hasNext()) {
	    		stream.writeInt(it.nextInteger());
	    	}	
		} else {
			DateIter it = c.select();
	    	while(it.hasNext()) {
	    		stream.writeInt(ToFromIntDate.INSTANCE.applyAsInt(it.nextDate()));
	    	}
		}
	}

	@Override public void visit(ObjectCol mixedList) throws IOException {
		stream.write(0); // type
		stream.writeByte(0);
		stream.writeInt(mixedList.size());
		for(int i=0; i<mixedList.size(); i++) {
			visit(mixedList.get(i));
		}
	}


	@Override public void visit(Object[] objectArray) throws IOException {
		stream.write(0); // type
		stream.writeByte(0);
		stream.writeInt(objectArray.length);
		for(int i=0; i<objectArray.length; i++) {
			visit(objectArray[i]);
		}
	}
}
