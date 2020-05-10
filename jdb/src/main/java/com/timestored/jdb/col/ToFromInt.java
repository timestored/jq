package com.timestored.jdb.col;

import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

public interface ToFromInt<T> extends IntFunction<T>, ToIntFunction<T> {
}
