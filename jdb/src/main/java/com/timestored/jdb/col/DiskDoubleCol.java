package com.timestored.jdb.col;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import javax.management.RuntimeErrorException;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Preconditions;
import com.timestored.jdb.function.DoublePredicate;
import com.timestored.jdb.iterator.DoubleIter;
import com.timestored.jdb.iterator.Locations;

import lombok.ToString;

@ToString
class DiskDoubleCol extends BaseDoubleCol {
	
	private Buff buff;
	private final long vectorOffset;
	/**TYPE=STRING private final StringMap stringMap; **/
	
	private long offset;
	private int mappedStartLocation;
	/** The number of items contained in this Col, only initialised when necessary to prevent unnecessary reads **/
	private int size = -1;
	private MappedByteBuffer mbb;
	private int mappedMin = Integer.MAX_VALUE;
	private int mappedMax = Integer.MIN_VALUE;
	private RMode mappedRMode = RMode.READ;
	
	DiskDoubleCol(File file, long fileOffset /**TYPE=STRING ,StringMap stringMap **/) {
		Preconditions.checkNotNull(file);
		Preconditions.checkArgument(file.isFile() && file.exists());
		/**TYPE=STRING this.stringMap = Preconditions.checkNotNull(stringMap); **/
		this.vectorOffset = fileOffset + 4; // first 4 is vector length as an int
		buff = new Buff(file); 
	}
	
	@Override public boolean isAppendable() { return true; }
	@Override public boolean isUpdateable() { return true; }
	
	
	@Override public int size() {
		try {
			// lazy reading of size() when needed as this is likely to be costly.
			if(size == -1) {
				// TODO opening this in write mode to force file creation. Bound to be nicer way.
				MappedByteBuffer mbb = buff.map(4, vectorOffset - 4, RMode.WRITE);
				size = mbb.getInt();
				close();
			}
			return size;
		} catch(IOException e) {
			throw new IllegalStateException("size call failed due to buff.map", e);
		}
	}
	
	@Override public void map(Locations locations, RMode rmode) throws IOException {
		if(locations.isEmpty()) {
			return;
		}
		// if the requested locations are outside those previously mapped, time to remap.
		if(!rmode.equals(mappedRMode) || (locations.getMin() < mappedMin || locations.getMax() > mappedMax)) {
			// first check for appends, if appending we need to update the size stored on disk
			// read size, save new to disk
			int sz = locations.getMax() + 1;
			if(sz > size) {
				mbb = buff.map(4, vectorOffset - 4, RMode.WRITE);
				mbb.putInt(sz);
				size = sz;
			}

			// expand the mapping by one either side if possible
			// this allows the sorted check during add to be ran easier and faster.
			int lMin = locations.getMin();
			int lMax = locations.getMax();
			if(lMin > 0) {
				lMin--;
			}
			if(lMax < size - 1) {
				lMax++;
			}
			
			mappedStartLocation = lMin;
			offset = vectorOffset + mappedStartLocation * getSizeInBytes();
			int sizeInBytes = (1 + lMax - mappedStartLocation) * getSizeInBytes();
			mbb = buff.map(sizeInBytes, offset, rmode);
			
			mappedMin = locations.getMin();
			mappedMax = locations.getMax();
		}
	}

	@Override public void setSize(int newSize) throws IOException {
		int sz = size();
		if(newSize > sz) {
			map(Locations.forRange(newSize-1, newSize), RMode.WRITE);
		} else if(newSize < sz) {
			mbb = buff.map(4, vectorOffset - 4, RMode.WRITE);
			mbb.putInt(newSize);
			close();
			size = newSize;
			buff.trimToSize(vectorOffset + (newSize * getSizeInBytes()));
		}
	}

	@Override public void close() throws IOException {
		mappedMin = Integer.MAX_VALUE;
		mappedMax = Integer.MIN_VALUE;
		mbb = null;
		if(buff != null) {
			buff.close();
		}
	}
	

	@Override void uncheckedSet(int index, double value) throws IOException {	
		int pos = (int) ((index - mappedStartLocation) * getSizeInBytes());
		mbb.putDouble(pos, /**TYPE=STRING stringMap.intern(**/value/**TYPE=STRING )**/);
	}
	
	@Override public double get(int index) {
		int pos = (int) ((index - mappedStartLocation) * getSizeInBytes());
		return /**TYPE=STRING stringMap.expand(**/mbb.getDouble(pos)/**TYPE=STRING )**/; 
	}

	
	@Override public boolean uncheckedAddAll(DoubleIter myDoubleIterator) throws IOException {
		
		int numAdding = myDoubleIterator.size();
		if(numAdding > 0) {
			if(size == -1) {
				mbb = buff.map(4, vectorOffset - 4, RMode.WRITE);
				size = mbb.getInt();
				close();
			}
			
			int origSize = size;
			map(Locations.forRange(size, size + numAdding), RMode.WRITE);
			
			while(myDoubleIterator.hasNext()) {
				uncheckedSet(origSize++, myDoubleIterator.nextDouble());
			}
			close();
			return true;
		}
		
		return false;
	}

	@Override public DoubleCol sort() {
		MemoryDoubleCol m = new MemoryDoubleCol(this.size());
		try {
			m.addAll(this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		m.sort();
		try {
			map(Locations.upTo(this.size()), RMode.WRITE);
			for(int i=0; i<this.size; i++) {
				set(i, m.get(i));
			}
			close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return this;
	}

}
