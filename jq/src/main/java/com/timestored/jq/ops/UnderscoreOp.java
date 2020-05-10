package com.timestored.jq.ops;

import java.util.Optional;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.iterator.RangeLocations;
import com.timestored.jq.TypeException;

public class UnderscoreOp extends BaseDiad {
	public static UnderscoreOp INSTANCE = new UnderscoreOp(); 
	@Override public String name() { return "_"; }

	@Override public Object run(Object a, Object b) {
		Optional<Integer> n = CastOp.CAST.intUnsafe(a);
		Optional<IntegerCol> chopcol = CastOp.CAST.iColUnsafe(a);
		
		// Remove first/last n entries
		if(n.isPresent()) {
			if(b instanceof Mapp) {
				Mapp m = (Mapp) b;
				return new MyMapp(ex(n.get(),m.getKey()), ex(n.get(), m.getValue()));
			} else if(b instanceof Col) {
				return ex(n.get(), (Col) b);	
			}
		} else if(chopcol.isPresent() && b instanceof Col) {
			return ex(chopcol.get(), (Col) b);
		}
		
		throw new TypeException();
	}

	public Col ex(IntegerCol chopPositions, Col source) {
		MemoryObjectCol moc = new MemoryObjectCol(chopPositions.size());
		for(int i=0; i<chopPositions.size(); i++) {
			int lower = chopPositions.get(i);
			int upper = i == chopPositions.size()-1 ? source.size(): chopPositions.get(i+1);
			Col val = source.select(new RangeLocations(lower, upper));
			moc.set(i, val);
		}
		return moc;
	}

	public Col ex(int n, Col col) {
		if(Math.abs(n) >= col.size()) {
			return ColProvider.emptyCol(col.getType());
		} else if(n == 0) {
			return col;
		} else {
			int lower = n < 0 ? 0 : n;
			int upper = n < 0 ? col.size()+n : col.size();
			return col.select(new RangeLocations(lower,upper));
		}
	}
}