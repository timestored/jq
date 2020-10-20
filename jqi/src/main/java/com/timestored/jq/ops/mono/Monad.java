package com.timestored.jq.ops.mono;

import com.timestored.jq.RankException;
import com.timestored.jq.ops.Op;

public interface Monad extends Op {
    public Object run(Object a);
    
    @Override default Object run(Object[] args) {
    	if(args.length > 1) {
    		throw new RankException();
    	}
    	return args.length == 1 ? run(args[0]) :  this; 
    }

}
