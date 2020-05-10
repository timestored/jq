package com.timestored.jq.ops;

import static com.timestored.jq.ops.CastOp.CAST;

import com.timestored.jdb.col.BooleanCol;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.database.CType;
import com.timestored.jq.Context;
import com.timestored.jq.Frame;
import com.timestored.jq.TypeException;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.TypeOp;

import lombok.Getter;
import lombok.Setter;

public abstract class BaseCastingDiadUniform extends BaseDiadUniform {

	@Getter @Setter protected Frame frame;
	@Getter @Setter protected Context context;
	
	@Override public boolean ex(boolean a, boolean b) {
        return CastOp.CAST.b(ex(CAST.i(a), CAST.i(b)));
    }
    @Override public BooleanCol ex(boolean a, BooleanCol b) {
        return CastOp.CAST.b(ex(CAST.i(a), CAST.i(b)));
    }
    @Override public BooleanCol ex(BooleanCol a, boolean b) {
        return CastOp.CAST.b(ex(CAST.i(a), CAST.i(b)));
    }
    @Override public BooleanCol ex(BooleanCol a, BooleanCol b) {
        return CastOp.CAST.b(ex(CAST.i(a), CAST.i(b)));
    }
    

	@Override public Object ex(Object a, ObjectCol b) {
		short typ = TypeOp.TYPE.type(a);
		if(b.size() == 0 && (typ<=0 || CountOp.INSTANCE.count(a)==0)) {
			return ColProvider.emptyCol(CType.OBJECT);
		}
		// Else check size and each
		throw new TypeException(); 
	}
	@Override public Object ex(ObjectCol a, Object b) { 
		short rightTyp = TypeOp.TYPE.type(b);
		if(a.size() == 0 && (rightTyp<=0 || CountOp.INSTANCE.count(b)==0)) {
			return ColProvider.emptyCol(CType.OBJECT);
		}
		// Else check size and each
		throw new TypeException(); 
	}
	
}
