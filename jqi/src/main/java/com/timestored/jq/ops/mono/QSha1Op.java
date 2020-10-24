package com.timestored.jq.ops.mono;

import java.text.NumberFormat;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ByteCol;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.col.FloatCol;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.LongCol;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.ShortCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Minute;
import com.timestored.jdb.database.Month;
import com.timestored.jdb.database.Second;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timespan;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jdb.kexception.KException;
import com.timestored.jq.Context;
import com.timestored.jq.Frame;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.EachOp;
import com.timestored.jq.ops.EqualOp;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.Op;
import com.timestored.jq.ops.SublistOp;

import lombok.Getter;
import lombok.Setter;

import static com.timestored.jq.ops.mono.Qs1Op.B3;

public class QSha1Op implements Monad {
	public static QSha1Op INSTANCE = new QSha1Op(); 
	@Override public String name() { return ".Q.sha1"; }
	
	@Override
	public short typeNum() {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void setFrame(Frame frame) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setContext(Context context) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Object run(Object a) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
