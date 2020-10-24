package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;

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
        } else if(o instanceof Character) {
            return ex((char) o);  	
        } else if(o instanceof ObjectCol && CountOp.INSTANCE.count(o)==0) {
        	return ColProvider.emptyObjectCol;
        }
        return mapEach(o);
    }

    public StringCol ex(StringCol a) {
    	return a.map(s -> s.toLowerCase());
    }

    public CharacterCol ex(CharacterCol a) {
    	return a.map(c -> Character.toLowerCase(c));
    }
    
    public String ex(String a) { return a.toLowerCase(); }
    public char ex(char a) { return Character.toLowerCase(a);}

}
