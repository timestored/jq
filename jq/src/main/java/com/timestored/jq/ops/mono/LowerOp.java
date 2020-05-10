package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.MemoryCharacterCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jq.TypeException;

public class LowerOp extends BaseMonad {
	public static LowerOp INSTANCE = new LowerOp();
	@Override public String name() { return "lower"; }

	@Override public Object run(Object o) {
        if(o instanceof StringCol) {
            return ex((StringCol) o);  	
        } else if(o instanceof String) {
            return ex((String) o);  	
        } else if(o instanceof CharacterCol) {
            return ex((CharacterCol) o);  	
        } if(o instanceof Character) {
            return ex((char) o);  	
        }  
        throw new TypeException();
    }

    public StringCol ex(StringCol a) {
    	if(a.size() == 0) {
    		return a;
    	}
    	StringCol r = new MemoryStringCol(a.size());
    	for(int i=0; i<a.size(); i++) {
    		r.set(i, a.get(i).toLowerCase());
    	}
    	return r;
    }


    public CharacterCol ex(CharacterCol a) {
    	if(a.size() == 0) {
    		return a;
    	}
    	CharacterCol r = new MemoryCharacterCol(a.size());
    	for(int i=0; i<a.size(); i++) {
    		r.set(i, Character.toLowerCase(a.get(i)));
    	}
    	return r;
    }
    
    public String ex(String a) { return a.toLowerCase(); }
    public char ex(char a) { return Character.toLowerCase(a);}

}
