package com.timestored.jq.ops.mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.DomainException;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.CastOp;

public class KeyOp extends BaseMonad {
	public static KeyOp INSTANCE = new KeyOp();
	@Override public String name() { return "key"; }

	@Override public Object run(Object a) {
		if(a instanceof Mapp) {
			return ((Mapp)a).getKey();
		} else if(a instanceof Col) {
			return CType.getType(((Col)a).getType()).getQName();
		} else if(a instanceof String) {
			return ex((String) a);
		} else if(a instanceof Long) {
			return TilOp.INSTANCE.til((long) a);
		} else if(a instanceof Integer) {
			return TilOp.INSTANCE.til((int) a);
		} else if(a instanceof Short) {
			return TilOp.INSTANCE.til((short) a);
		} else if(a instanceof Boolean) {
			return TilOp.INSTANCE.til((boolean) a);
		}
		throw new TypeException("Expected whole number. Got: " + a.toString());
	}

	private Object ex(String fileOrFolderPath) {
		Path p = HopenOp.toPath(fileOrFolderPath);
		try {
			if(Files.isDirectory(p)) {
				List<String> sl = Files.list(p).map(pth -> pth.getFileName().toString()).collect(Collectors.toList());
				return CastOp.CAST.s(sl);
			} else if(Files.exists(p)) {
				return fileOrFolderPath;
			}
		} catch(IOException e) {
			throw new DomainException(e);
		}
		return ColProvider.emptyCol(CType.OBJECT);
	}
}
