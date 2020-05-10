package com.timestored.jq.ops; 
import static com.timestored.jdb.database.SpecialValues.*;

import com.timestored.jdb.col.*;

import com.timestored.jdb.kexception.LengthException;
import com.timestored.jq.TypeException;


public class DollarOp extends BaseDiad {
	public static DollarOp INSTANCE = new DollarOp();
	@Override public String name() { return "$"; }
	
	@Override public Object run(Object a, Object b) {
		// Objects about to get complicated
        if(a instanceof Character) {
        	char c = (Character) a;
            if(Character.isUpperCase(c)) {
                return ParseOp.PARSE.run(a, b);
            }
        }
        return CastOp.CAST.run(a, b);
	}
}