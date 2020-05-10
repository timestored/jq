package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jq.TypeException;

public class UpperOp extends BaseMonad {
	public static UpperOp INSTANCE = new UpperOp();
	@Override public String name() { return "upper"; }

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
    	return a.map(s -> s.toUpperCase());
    }

    public CharacterCol ex(CharacterCol a) {
    	return a.map(c -> Character.toUpperCase(c));
    }
    
    public String ex(String a) { return a.toUpperCase(); }
    public char ex(char a) { return Character.toUpperCase(a);}
}
