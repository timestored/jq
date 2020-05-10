package com.timestored.jq.ops.mono;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.Op;

public interface Monad extends Op {
    public Object run(Object a);

}
