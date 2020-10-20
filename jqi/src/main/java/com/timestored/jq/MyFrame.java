package com.timestored.jq;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.Database;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.OpRegister;

import lombok.Getter;

public class MyFrame implements Frame {
	
    private LinkedHashMap<String,Object> store = new LinkedHashMap<>();
    @Getter private final Frame outerFrame;
    
    public MyFrame(Frame parentFrame) {
		this.outerFrame = parentFrame;
	}
    
    public MyFrame() { 
		this.outerFrame = null; 
    }
    
    @Override
	public Object get(String id) {
		if(id.trim().isEmpty()) {
			return this;
		}
		
    	int p = id.indexOf(".");
    	if(p == -1 || id.equals(".")) {
    		Object v = store.get(id);
        	if(v == null && outerFrame != null) {
    			v = outerFrame.get(id);
        	}
        	if(v == null) {
    			v = OpRegister.ops.get(id);
        	}
        	if(v == null) {
        		throw new IdNotFoundException(id);
        	}
        	return v;	
    	} else {
    		String[] nsSub = getNsSubnamePair(id);
    		Object nso = store.get(nsSub[0]);
    		if(nso == null) {
				throw new TypeException("Namespace doesn't exist:" + id);
    		} else if(nso instanceof Frame) {
    			return ((Frame) nso).get(nsSub[1]);
    		} else if(nso instanceof Tbl) {
    			return ((Tbl) nso).getCol(nsSub[1]);
    		} else if(nso instanceof Mapp) {
    			Col c = ((Mapp) nso).getKey();
    			if(c instanceof StringCol) {
    				return IndexOp.INSTANCE.run(nso, nsSub[1]);
    			}
    		}
			throw new TypeException("Tried to index into invalid frame:" + id);
    	}
    	
    	
    }
    
    private static final String[] rootNS = new String[] { "",""};
    
    public static String[] getNsSubnamePair(String id) {
    	if(id == null || id.isEmpty() || id.trim().equals(".")) {
    		return rootNS;
    	}
		// Formats include var .ns.a t.col .ns  .ns.ms.goo
    	String sid = id.charAt(0) == '.' ? id.substring(1) : id;
		int p = sid.indexOf(".");
		boolean end = p==-1;
		String ns = end ? sid : sid.substring(0, p);
		String subname = end ? "" : sid.substring(p+1);
		return new String[] { ns, subname };
    }
    
    @Override
	public void assign(String id, Object value) {
    	int p = id.indexOf(".");
    	if(p == -1) {
    		store.put(id, value);
    	} else {
    		// Get Frame for .a.b   tbl.col   formats
    		String[] nsSub = getNsSubnamePair(id);
    		Object nso = store.computeIfAbsent(nsSub[0], k -> new MyFrame(this));
    		if(nso instanceof Frame) {
    			((Frame) nso).assign(nsSub[1], value);
    		} else {
				throw new TypeException("Tried to assign into invalid frame:" + id);
    		}
    	}
    }
    

	@Override
	public Mapp getMapp() {
    	Set<String> ks = store.keySet();
    	StringCol keys = ColProvider.toStringCol(ks);
    	ObjectCol vals = new MemoryObjectCol(store.size());
    	int i = 0;
    	for(String k : ks) {
    		vals.set(i++, store.get(k));
    	}
    	return new MyMapp(keys, vals);
    	
    }

	@Override public StringCol getKeys() { return ColProvider.toStringCol(store.keySet()); }

	@Override public Mapp getMapp(String ns) {
		Object nso = get(ns);
		if(nso instanceof Frame) {
			return ((Frame) nso).getMapp();
		} else if(nso instanceof Tbl) {
			return ((Tbl) nso);
		} else if(nso instanceof Mapp) {
			Col c = ((Mapp) nso).getKey();
			if(c instanceof StringCol) {
				return ((Mapp) nso);
			}
		}
		throw new TypeException("Can only find variables for namespace. Not: " + ns);
	}

}
