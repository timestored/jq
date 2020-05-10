package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.*;
import com.timestored.jq.RankException;
import com.timestored.jq.ops.CastOp;

//import it.unimi.dsi.fastutil.booleans.BooleanArrays;
//import it.unimi.dsi.fastutil.chars.CharArrays;
//import it.unimi.dsi.fastutil.doubles.DoubleArrays;
//import it.unimi.dsi.fastutil.floats.FloatArrays;
//import it.unimi.dsi.fastutil.ints.IntArrays;
//import it.unimi.dsi.fastutil.longs.LongArrays;
//import it.unimi.dsi.fastutil.shorts.ShortArrays;

import java.util.Arrays;

public class IAscOp extends BaseMonad {
	public static IAscOp INSTANCE = new IAscOp();
	@Override public String name() { return "iasc"; }


    public Object run(Object o) {
    	if(TypeOp.TYPE.type(o) < 0) {
    		throw new RankException();
    	}

    	if(o instanceof FloatCol) {
            return ex((FloatCol) o);  	
        } else if(o instanceof ShortCol) {
            return ex((ShortCol) o);  	
        } else if(o instanceof DoubleCol) {
            return ex((DoubleCol) o);  	
        } else if(o instanceof BooleanCol) {
            return ex((BooleanCol) o);  	
        } else if(o instanceof CharacterCol) {
            return ex((CharacterCol) o);  	
        } else if(o instanceof LongCol) {
            return ex((LongCol) o);  	
        } else if(o instanceof IntegerCol) {
            return ex((IntegerCol) o);  	
        } 
        throw new UnsupportedOperationException();
    }

//    public LongCol ex(BooleanCol a)    { IntegerCol perm = TilOp.INSTANCE.til(a.length); BooleanArrays.quickSortIndirect(perm, a); return CastOp.CAST.j(perm);}
  public LongCol ex(BooleanCol a)  { throw new UnsupportedOperationException(); }
  public LongCol ex(CharacterCol a){ throw new UnsupportedOperationException(); }
  public LongCol ex(ShortCol a)    { throw new UnsupportedOperationException(); }
  public LongCol ex(IntegerCol a)  { throw new UnsupportedOperationException(); }
  public LongCol ex(LongCol a)     { throw new UnsupportedOperationException(); }
  public LongCol ex(FloatCol a)    { throw new UnsupportedOperationException(); }
  public LongCol ex(DoubleCol a)   { throw new UnsupportedOperationException(); }

}

