package com.timestored.jdb.col;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.google.common.collect.Lists;

import lombok.ToString;

/**
 * Maps strings to a unique integer allowing efficient mapping from int<->String back and forth.
 * (Should be faster int->String).
 */
@ToString
public class StringMap implements Serializable,ToFromInt<String> {
	
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_SIZE = 1024*8;
	public static final int NULL = -1;
	private final List<String> strings;
	private final ObjectIntHashMap<String> stringToInt;
	public static StringMap INSTANCE = new StringMap();  
	
	private StringMap() {
		strings = Lists.newArrayListWithExpectedSize(DEFAULT_SIZE);
		stringToInt = new ObjectIntHashMap<String>();
	}
	
	
	@Override public int applyAsInt(@Nullable String s) {
		if(s == null) {
			return NULL;
		}
		int v = stringToInt.getOrDefault(s, NULL);
		if(v == NULL) {
			v = strings.size();
			stringToInt.put(s, v);
			strings.add(s);
		}
		return v;
	}

	@Override  public String apply(int n) { return n==-1 ? null : strings.get(n); }
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
	    throw new InvalidObjectException("Proxy required");
	}

	private Object writeReplace() { return new SerializationProxy(this); }
	
	private static class SerializationProxy implements Serializable {
		
		private static final long serialVersionUID = 1L;
		private transient StringMap sm;

	    SerializationProxy(StringMap sm) {
	        this.sm = sm;
	    }

		private void writeObject(ObjectOutputStream oos) throws IOException {
		    oos.defaultWriteObject();
		    for(String s : sm.strings) {
		    	byte[] buf = s.getBytes();
		    	oos.writeInt(buf.length);
		    	oos.write(buf);
		    }
		}

		private void readObject(ObjectInputStream stream) throws ClassNotFoundException, IOException {
			stream.defaultReadObject();
			sm = INSTANCE;
			sm.strings.clear();
			sm.stringToInt.clear();
			int i = 0;
			do {
				int l = stream.readInt();
				byte[] buf = new byte[l];
				stream.read(buf);
				String s = new String(buf);
				sm.strings.add(s);
				sm.stringToInt.put(s, i++);
			} while(stream.available() > 0);
		}

	    private Object readResolve() { return sm; }
	}

}
