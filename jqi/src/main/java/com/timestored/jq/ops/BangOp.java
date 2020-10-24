package com.timestored.jq.ops;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


import com.google.common.net.InetAddresses;
import com.timestored.jdb.col.CharacterCol;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryByteCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MemoryStringCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.StringCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jdb.kexception.LengthException;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jdb.kexception.OsException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.ColsOp;
import com.timestored.jq.ops.mono.HcountOp;
import com.timestored.jq.ops.mono.KeysOp;
import com.timestored.jq.ops.mono.QqtOp;
import com.timestored.jq.ops.mono.Qs1Op;
import com.timestored.jq.ops.mono.QsOp;

public class BangOp extends BaseDiad {
	public static BangOp INSTANCE = new BangOp(); 
	@Override public String name() { return "!"; }

	@Override public Object run(Object a, Object b) {
		if(a instanceof Col && b instanceof Col) {
			return new MyMapp((Col)a, (Col)b);
			
//			-4!x        tokens                  Replaced:
//				-8!x        to bytes                 -1!   hsym
//				-9!x        from bytes               -2!   attr
//				-10!x       type enum                -3!   .Q.s1
//				-11!        streaming execute        -5!   parse
//				-14!x       quote escape             -6!   eval
//				-16!x       ref count                -7!   hcount
//				-18!x       compress byte            -12!  .Q.host
//				-19!        compress file            -13!  .Q.addr
//				-21!x       compression stats        -15!  md5
//				-22!x       uncompressed length      -20!  .Q.gc
//				-23!x       memory map               -24!  reval
//				-25!x       async broadcast          -29!  .j.k
//				-26!x       SSL                      -31!  .j.jd
//				-27!(x;y)   format
//				-30!x       deferred response
//				-33!x       SHA-1 hash
//				-36!(x;y)   load master key
//				-120!x      memory domain
		} else if(a instanceof Long) {
			try {
				int l = CastOp.CAST.i((long) a);
				return ex(l, b);
			} catch (IOException e) {
				throw new OsException(e);
			} 
		}
		throw new TypeException();
	}

	Object ex(int l, Object b) throws IOException {
			
		if(l>=0 && (b instanceof Tbl || (QqtOp.INSTANCE.ex(b)))) {
			StringCol cols = ColsOp.INSTANCE.ex(b);
			if(cols.size() <= l) {
				throw new LengthException("Trying to key by " + l + " columns when table only has " + cols.size());
			}
			
			if(l == KeysOp.INSTANCE.ex(b).size()) {
				return b;
			}
			
			Tbl tbl = null;
			if(b instanceof Tbl) {
				tbl = (Tbl)b;
			} else if(b instanceof Mapp) {
				Mapp m = (Mapp)b;
				ObjectCol ocA = ((Tbl) m.getKey()).getValue();
				ObjectCol ocB = ((Tbl) m.getValue()).getValue();
				ObjectCol ocC = new MemoryObjectCol(0);
				ocC.addAll(ocA);
				ocC.addAll(ocB);
				tbl = new MyTbl(cols, ocC);	
			}
			if(l == 0) {
				return tbl;
			}
			int i=0;
			// Convert first l columns to table
			MemoryStringCol headers = new MemoryStringCol(l);
			MemoryObjectCol vals = new MemoryObjectCol(l);
			for(;i<l;i++) {
				headers.set(i, tbl.getKey().get(i));
				vals.set(i, tbl.getValue().get(i));
			}
			Tbl keysTbl = new MyTbl(headers, vals);
			// Convert remaining columns to table
			headers = new MemoryStringCol(cols.size()-l);
			vals = new MemoryObjectCol(cols.size()-l);
			for(int j=0;i<cols.size();i++,j++) {
				headers.set(j, tbl.getKey().get(i));
				vals.set(j, tbl.getValue().get(i));
			}
			Tbl valsTbl = new MyTbl(headers, vals);
			return new MyMapp(keysTbl, valsTbl);
		}
	
		switch(l) {
			case SpecialValues.ni: context.stdout(QsOp.INSTANCE.asText(b)); return b; 
			case -3:  return Qs1Op.B3.run(b);
			case -7:  return HcountOp.INSTANCE.run(b);
			case -12: return InetAddresses.fromInteger((int) b).getHostName();
			case -13: return getIP((String) b);
			case -20: System.gc(); return 0l;
			case -33: return toSHA1((CastOp.CAST.s((CharacterCol)b)).toString());
			// Our own custom entries:
			case -200: return getWebPage((String) CastOp.CAST.run('s', b));
			case -201:
				QsOp.INSTANCE.setContext(context);
				QsOp.INSTANCE.setFrame(frame);
				return QsOp.INSTANCE.run(b);
			case -202:
				QqtOp.INSTANCE.setContext(context);
				QqtOp.INSTANCE.setFrame(frame);
				return QqtOp.INSTANCE.run(b);
		}
		throw new UnsupportedOperationException();
	}
  	
	private ObjectCol getWebPage(String httpPage) {
		try {
			if(httpPage.charAt(0) == ':') {
				httpPage = httpPage.substring(1);
			}
			URL oracle = new URL(httpPage);
	        BufferedReader in = new BufferedReader(new InputStreamReader(oracle.openStream()));
	        List<String> r = new ArrayList<>();
	        String l = null;
	        while ((l = in.readLine()) != null) { r.add(l); }
	        in.close();
	        return ColProvider.toCharacterCol(r.toArray(new String[0]));
		} catch (IOException e) {
			throw new DomainException(e.getLocalizedMessage());
		}
	}

	private int getIP(String hostname) {
		try {
			return InetAddresses.coerceToInteger(InetAddress.getByName(hostname));
		} catch (UnknownHostException e) {
			return -1;
		}
	}
	
	private static MemoryByteCol toSHA1(String txt) {
	    MessageDigest md = null;
	    try {
	        md = MessageDigest.getInstance("SHA-1");
	    } catch(NoSuchAlgorithmException e) {
	    	throw new NYIException(e.toString());
	    }
	    return new MemoryByteCol(md.digest(txt.getBytes()));
	}
}