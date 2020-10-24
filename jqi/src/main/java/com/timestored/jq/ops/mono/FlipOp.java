package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jq.RankException;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.EachOp;
import com.timestored.jq.ops.EqualOp;
import com.timestored.jq.ops.IndexOp;

public class FlipOp extends BaseMonad {
	public static FlipOp INSTANCE = new FlipOp();
	@Override public String name() { return "flip"; }

	@Override public Object run(Object a) {
		if(TypeOp.TYPE.type(a) < 0) {
			throw new RankException("Can't flip an atom");
		} else if(a instanceof Tbl) {
			Tbl tbl = (Tbl) a;
			return new MyMapp(tbl.getKey(), tbl.getValue());
		} else if(a instanceof Mapp) {
			if(((Mapp) a).isKeyedTable()) {
				throw new NYIException();
			}
			return new MyTbl((Mapp) a);
		} else if(TypeOp.TYPE.type(a) == 0) {
			return flip((ObjectCol) a);
		}
		throw new NYIException();
	}

	public ObjectCol flip(ObjectCol original) {
		long cols = original.size();
		if(cols == 0) {
			return original;
		}
		long rows = getSquareCount((ObjectCol) original);
		if(rows == 0) {
			throw new LengthException();
		}
		// Flip ColsxRows 2x3 (1 2 3;4 5 6) to 3x2 (1 4;2 5;3 6)
		MemoryObjectCol outer = new MemoryObjectCol((int) rows);
		for(int r=0; r<rows; r++) {
			outer.set(r, new MemoryObjectCol((int) cols));
		}
		for(int r=0; r<rows; r++) {
			for(int c=0; c<cols; c++) {
				Object val = IndexOp.INSTANCE.run(IndexOp.INSTANCE.ex(original, c), r);
				((MemoryObjectCol) outer.get(r)).set(c, val);
			}
		}
		for(int r=0; r<rows; r++) {
			outer.set(r, CastOp.flattenGenericIfSameType(((MemoryObjectCol) outer.get(r))));
		}
		return outer;
	}

	/**
	 * @return The length of all items (same number) if "square" else 0.
	 */
	public static long getSquareCount(ObjectCol col) {
		if(col.size() == 0) {
			return 0;
		}
		long itemCount = CountOp.INSTANCE.count(col.get(0));
		if(itemCount > 1
			&& (boolean) AllOp.INSTANCE.run(EqualOp.INSTANCE.run(itemCount, EachOp.INSTANCE.run(CountOp.INSTANCE, col)))) {
			return itemCount;
		}
		return 0;
	}
}
