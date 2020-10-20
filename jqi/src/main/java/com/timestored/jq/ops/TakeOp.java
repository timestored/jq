package com.timestored.jq.ops;

import java.util.Optional;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.function.ToIntegerFunction;
import com.timestored.jdb.iterator.Locations;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.EnlistOp;
import com.timestored.jq.ops.mono.TypeOp;

import lombok.RequiredArgsConstructor;

public class TakeOp extends BaseDiad {
	public static TakeOp INSTANCE = new TakeOp(); 
	@Override public String name() { return "#"; }

	@Override public Object run(Object a, Object b) {
				
		if(a instanceof String) {
			String s = (String)a;
			if(s.equals("s") && b instanceof Col) {
				Col c = (Col)b;
				if(c.applySorted()) {
					return c;
				} else {
					throw new TypeException("s-fail. wasn't sorted.");
				}
			}
			throw new DomainException("Only `s works for now");
		}

		Optional<Integer> n = CastOp.CAST.intUnsafe(a);
		// Take / Repeat
		if(n.isPresent()) {
			final int chop = n.get();
			Col c = null;
			short tb = TypeOp.TYPE.type(b);
			if(b instanceof Col) {
				c = (Col) b;
			} else if(tb < 0) {
				c = ColProvider.getInMemory(CType.getType(tb), 1);
				c.setObject(0, b);
			} else {
				c = (Col) EnlistOp.INSTANCE.run(b);
			}
			
			// we have a col
			if(c.size() == 0) {
				return ColProvider.emptyCol(CType.OBJECT);
			}
			final int size = Math.abs(chop);
			final int srcSize = c.size();
			if(chop < 0) {
				final int offset = chop>0 ? 0 : (srcSize-(size%srcSize));
				return c.select(new MappedLocations(size, c.size()-1, idx -> (idx+offset) % srcSize));
			}
			return c.select(new MappedLocations(size, c.size()-1, idx -> idx % srcSize));
		}
		throw new TypeException();
	}
	

	@RequiredArgsConstructor
	private static class MappedLocations implements Locations {
		private final int size;
		private final int maxPositionReadFromCol;
		private final ToIntegerFunction<Integer> mapObjectToLocation;
		private int p = 0;
		
		@Override public int size() { return size; }

		@Override public void reset() { p=0; }
		@Override public boolean hasNext() { return p<size; }
		@Override public int nextInteger() { return mapObjectToLocation.applyAsInteger(p++); }
		@Override public Locations first(int n) { throw new UnsupportedOperationException();	}
		@Override public Locations last(int n) { throw new UnsupportedOperationException();	}
		@Override public int get(int idx) { return mapObjectToLocation.applyAsInteger(idx); }
		@Override public int getMin() { return 0; }
		@Override public int getMax() { return maxPositionReadFromCol; }
		@Override public Locations setBounds(int lowerBound, int upperBound) { throw new UnsupportedOperationException();	}
		
	}
}