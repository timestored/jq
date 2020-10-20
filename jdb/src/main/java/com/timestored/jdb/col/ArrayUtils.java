package com.timestored.jdb.col;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.floats.FloatArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.shorts.ShortArrays;

/**
 * Wraps array library cols to standardise naming of calls to allow code generation to work. 
 */
public class ArrayUtils {

	public static int[] iasc(double[] v) {
		int[] perm = til(v.length);
		DoubleArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(int[] v) {
		int[] perm = til(v.length);
		IntArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(float[] v) {
		int[] perm = til(v.length);
		FloatArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(byte[] v) {
		int[] perm = til(v.length);
		ByteArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(short[] v) {
		int[] perm = til(v.length);
		ShortArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(long[] v) {
		int[] perm = til(v.length);
		LongArrays.quickSortIndirect(perm, v);
		return perm;
	}

	public static int[] iasc(char[] v) {
		int[] mv = new int[v.length];
		for(int i=0; i<v.length; i++) {
			mv[i] = v[i];
		}
		return iasc(mv);
	}
	
	public static int[] iasc(boolean[] v) {
		int[] mv = new int[v.length];
		for(int i=0; i<v.length; i++) {
			mv[i] = v[i] ? 1 : 0;
		}
		return iasc(mv);
	}
	
	private static int[] til(int n) {
		int[] r = new int[n];
		for(int i=0; i<n; i++) {
			r[i] = i;
		}
		return r;
	}
}
