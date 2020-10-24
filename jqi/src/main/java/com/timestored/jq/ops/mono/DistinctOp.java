package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.col.MemoryDoubleCol;
import com.timestored.jdb.col.MemoryFloatCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryShortCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.iterator.BooleanIter;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.FloatIter;
import com.timestored.jdb.iterator.IntegerIter;
import com.timestored.jdb.iterator.LongIter;
import com.timestored.jdb.iterator.ObjectIter;
import com.timestored.jdb.iterator.ShortIter;
import com.timestored.jdb.iterator.StringIter;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

import it.unimi.dsi.fastutil.booleans.BooleanArraySet;
import it.unimi.dsi.fastutil.booleans.BooleanSet;
import it.unimi.dsi.fastutil.doubles.DoubleLinkedOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatLinkedOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.shorts.ShortLinkedOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;

public class DistinctOp extends MonadReduceToSameObject {
	public static DistinctOp INSTANCE = new DistinctOp();
	@Override public String name() { return "distinct"; }

	@Override public StringCol ex(StringCol a) {
		ObjectSet<String> ds = new ObjectLinkedOpenHashSet<>();
		for(StringIter di = a.select();di.hasNext();) {
			ds.add(di.nextString());
		}
		return new MemoryStringCol(ds.toArray(new String[] {})); 
	}

	@Override public ObjectCol ex(ObjectCol a) {
		ObjectSet<Object> ds = new ObjectLinkedOpenHashSet<>();
		for(ObjectIter di = a.select();di.hasNext();) {
			ds.add(di.nextObject());
		}
		return MemoryObjectCol.of(ds.toArray(new Object[] {})); 
	}

	@Override public BooleanCol ex(BooleanCol a) {
		BooleanSet ds = new BooleanArraySet();
		for(BooleanIter di = a.select();di.hasNext();) {
			ds.add(di.nextBoolean());
		}
		return new MemoryBooleanCol(ds.toBooleanArray()); 
	}

	@Override public CharacterCol ex(CharacterCol a) {
		return CastOp.CAST.c(ex(CastOp.CAST.i(a)));
	}

	@Override public ShortCol ex(ShortCol a) {
		ShortSet ds = new ShortLinkedOpenHashSet();
		for(ShortIter di = a.select();di.hasNext();) {
			ds.add(di.nextShort());
		}
		return new MemoryShortCol(ds.toShortArray()); 
	}

	@Override public IntegerCol ex(IntegerCol a) {
		IntSet ds = new IntLinkedOpenHashSet();
		for(IntegerIter di = a.select();di.hasNext();) {
			ds.add(di.nextInteger());
		}
		IntegerCol c = new MemoryIntegerCol(ds.toIntArray());
		c.setType(a.getType());
		return c;
	}
	
	@Override public LongCol ex(LongCol a) {
		LongSet ds = new LongLinkedOpenHashSet();
		for(LongIter di = a.select();di.hasNext();) {
			ds.add(di.nextLong());
		}
		LongCol c = new MemoryLongCol(ds.toLongArray());
		c.setType(a.getType());
		return c;
	}

	
	@Override public FloatCol ex(FloatCol a) {
		FloatSet ds = new FloatLinkedOpenHashSet();
		for(FloatIter di = a.select();di.hasNext();) {
			ds.add(di.nextFloat());
		}
		FloatCol c = new MemoryFloatCol(ds.toFloatArray());
		c.setType(a.getType());
		return c;
	}
	
	@Override public DoubleCol ex(DoubleCol a) {
		DoubleSet ds = new DoubleLinkedOpenHashSet();
		for(DoubleIter di = a.select();di.hasNext();) {
			ds.add(di.nextDouble());
		}
		DoubleCol c = new MemoryDoubleCol(ds.toDoubleArray());
		c.setType(a.getType());
		return c;
	}

	@Override public double ex(double a)	{ throw new TypeException(); }
	@Override public float ex(float a) 		{ throw new TypeException(); }
	@Override public byte ex(byte a)		{ throw new TypeException(); }
	@Override public ByteCol ex(ByteCol a)	{ throw new TypeException(); }
	@Override public char ex(char a)		{ throw new TypeException(); }
	@Override public boolean ex(boolean a)	{ throw new TypeException(); }
	@Override public short ex(short a)		{ throw new TypeException(); }
	@Override public int ex(int a)			{ throw new TypeException(); }
	@Override public long ex(long a)		{ throw new TypeException(); }
	
}
