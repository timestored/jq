package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.col.MemoryDoubleCol;
import com.timestored.jdb.col.MemoryFloatCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jdb.col.MemoryLongCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryShortCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
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
import it.unimi.dsi.fastutil.objects.Object2LongAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.shorts.ShortLinkedOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortSet;

public class GroupOp extends MonadReduceToObject {
	public static GroupOp INSTANCE = new GroupOp();
	@Override public String name() { return "group"; }

	@Override public StringCol ex(StringCol a) {
		Object2LongSortedMap<String> sm = new Object2LongAVLTreeMap<>();
		for(long l=0; l<a.size(); l++) {
			sm.put(a.get((int) l), l);
		}
		MemoryStringCol k = new MemoryStringCol(sm.keySet().toArray(new String[] {}));
		return k;
	}

	@Override public ObjectCol ex(ObjectCol a) {
		ObjectSet<Object> ds = new ObjectLinkedOpenHashSet<>();
		for(ObjectIter di = a.select();di.hasNext();) {
			ds.add(di.nextObject());
		}
		return MemoryObjectCol.of(ds.toArray(new Object[] {})); 
	}

	@Override public Mapp ex(BooleanCol a) {
		MemoryLongCol[] r = new MemoryLongCol[2];
		r[0] = new MemoryLongCol(0);
		r[1] = new MemoryLongCol(0);
		for(int i=0; i<a.size(); i++) {
			r[a.get(i) ? 1 : 0].add((long)i);
		}

		MemoryBooleanCol mbc = new MemoryBooleanCol(0);
		MemoryObjectCol oc = new MemoryObjectCol(0);
		if(r[0].size()>0) { 
			mbc.add(false);
			oc.add(r[0]);
		}
		if(r[1].size()>0) { 
			mbc.add(true);
			oc.add(r[1]);
		}
		return new MyMapp(mbc, oc);
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

	@Override public Object ex(double a)	{ throw new TypeException(); }
	@Override public Object ex(float a) 	{ throw new TypeException(); }
	@Override public Object ex(byte a)		{ throw new TypeException(); }
	@Override public Object ex(ByteCol a)	{ throw new TypeException(); }
	@Override public Object ex(char a)		{ throw new TypeException(); }
	@Override public Object ex(boolean a)	{ throw new TypeException(); }
	@Override public Object ex(short a)		{ throw new TypeException(); }
	@Override public Object ex(int a)		{ throw new TypeException(); }
	@Override public Object ex(long a)		{ throw new TypeException(); }
	
}
