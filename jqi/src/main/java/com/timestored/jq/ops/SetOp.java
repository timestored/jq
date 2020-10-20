package com.timestored.jq.ops;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.timestored.jdb.database.Database;
import com.timestored.jdb.database.IpcDataWriter;
import com.timestored.jq.TypeException;

public class SetOp extends BaseDiad {
	public static SetOp INSTANCE = new SetOp(); 
	@Override public String name() { return "set"; }

	@Override public Object run(Object a, Object b) {
		if(a instanceof String) {
			String sid = (String) a;
			if(sid.startsWith(":")) {
				String fp = sid.substring(1).replace("/", File.separator);
				File f = new File(fp);
				try {
					File pr = f.getParentFile();
					if(pr != null) {
						pr.mkdirs();
					}
					f.createNewFile();
					try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(f))) {
						IpcDataWriter dw = new IpcDataWriter(dos);
						dw.visit(b);
						dos.close();
					}
				} catch (IOException e) {
					throw new TypeException(e.toString());
				}
			}
			String id = (String) a;
			this.frame.assign(id, b);
            return id;
		} else if(Database.QCOMPATIBLE) {
    		return b;
    	}
    	throw new TypeException("key for setting wasn't a string.: " + a.toString());
	}
  	
}