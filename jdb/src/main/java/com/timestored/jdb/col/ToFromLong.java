package com.timestored.jdb.col;

import java.util.function.LongFunction;

import com.timestored.jdb.function.ToLongFunction;

public interface ToFromLong<T> extends LongFunction<T>, ToLongFunction<T> {
}
