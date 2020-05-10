package com.timestored.jq.ops;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.StringCol;
import com.timestored.jq.ops.mono.*;

public class OpRegister {

    public static Map<String, Monad> monads = new ConcurrentHashMap<>();
    public static Map<String, Diad> diads = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Op> ops = new ConcurrentHashMap<>();

    public static void main(String... args) {}
    
    private static void reg(Monad monad) {
    	Monad r = monads.putIfAbsent(monad.name(), monad);
    	if(r != null) {
    		throw new IllegalStateException("Duplicate named: " + monad.toString() + " when " + r.toString() + "exists");
    	}
    	regop(monad);
    }
    private static void reg(Diad diad) {
    	Diad r = diads.putIfAbsent(diad.name(), diad);
    	if(r != null) {
    		throw new IllegalStateException("Duplicate named: " + diad.toString() + " when " + r.toString() + "exists");
    	}
    	regop(diad);
    }

    private static void regop(Op op) {
    	Op r = ops.putIfAbsent(op.name(), op);
    	if(r != null) {
    		throw new IllegalStateException("Duplicate named: " + op.toString() + " when " + r.toString() + "exists");
    	}
    }
    
    static {
		// Monads
    	reg(HopenOp.INSTANCE);
    	reg(HdelOp.INSTANCE);
    	reg(HcountOp.INSTANCE);
    	reg(Read0Op.INSTANCE);
    	reg(FlipOp.INSTANCE);
    	reg(RandOp.INSTANCE);
    	reg(ShowOp.INSTANCE);
    	reg(ExitOp.INSTANCE);
    	reg(StringOp.INSTANCE);
    	reg(SystemOp.INSTANCE);
    	reg(NegOp.INSTANCE);
    	reg(KeyOp.INSTANCE);
    	reg(NotOp.INSTANCE);
    	reg(EnlistOp.INSTANCE);
    	reg(GetOp.INSTANCE);
    	monads.put("value", GetOp.INSTANCE);
    	reg(MaxOp.INSTANCE);
    	reg(MinOp.INSTANCE);
    	reg(FirstOp.INSTANCE);
    	reg(LastOp.INSTANCE);
    	reg(AttrOp.INSTANCE);
    	reg(cosOp.INSTANCE);
    	reg(sinOp.INSTANCE);
    	reg(tanOp.INSTANCE);
    	reg(acosOp.INSTANCE);
    	reg(asinOp.INSTANCE);
    	reg(atanOp.INSTANCE);
    	reg(expOp.INSTANCE);
    	reg(logOp.INSTANCE);
    	reg(sqrtOp.INSTANCE);
    	reg(reciprocalOp.INSTANCE);
    	reg(ReverseOp.INSTANCE);
    	reg(IAscOp.INSTANCE);
    	reg(AscOp.INSTANCE);
    	reg(DescOp.INSTANCE);
    	reg(IDescOp.INSTANCE);
    	reg(TrimOp.INSTANCE);
    	reg(LTrimOp.INSTANCE);
    	reg(RTrimOp.INSTANCE);
    	reg(AbsOp.INSTANCE);
    	reg(AllOp.INSTANCE);
    	reg(AnyOp.INSTANCE);
    	reg(AvgOp.INSTANCE);
    	reg(TypeOp.TYPE);
    	reg(CountOp.INSTANCE);
    	reg(TilOp.INSTANCE);
    	reg(VarOp.INSTANCE);
    	reg(DevOp.INSTANCE);
    	reg(SvarOp.INSTANCE);
    	reg(SdevOp.INSTANCE);
    	reg(NullOp.INSTANCE);
    	reg(AvgsOp.INSTANCE);
    	reg(FloorOp.INSTANCE);
    	reg(CeilingOp.INSTANCE);
    	reg(WhereOp.INSTANCE);
    	reg(UpperOp.INSTANCE);
    	reg(LowerOp.INSTANCE);
    	reg(NiladicOp.INSTANCE);

    	// Diads
    	reg(TakeOp.INSTANCE);
    	reg(CutOp.INSTANCE);
    	reg(XexpOp.INSTANCE);
    	reg(UnderscoreOp.INSTANCE);
    	reg(QuestionOp.INSTANCE);
    	reg(EachOp.INSTANCE);
    	reg(PeachOp.INSTANCE);
    	reg(DotOp.INSTANCE);
    	reg(FillOp.INSTANCE);
    	reg(AndOp.INSTANCE);
    	reg(OrOp.INSTANCE);
    	reg(SublistOp.INSTANCE);
    	reg(SetOp.INSTANCE);
    	reg(AssignOp.INSTANCE);
    	reg(BangOp.INSTANCE);
    	reg(ModOp.INSTANCE);
    	reg(AddOp.INSTANCE);
    	reg(SubOp.INSTANCE);
    	reg(MulOp.INSTANCE);
    	reg(EqualOp.INSTANCE);
    	reg(GreaterThanOp.INSTANCE);
    	reg(GreaterThanOrEqualOp.INSTANCE);
    	reg(LessThanOrEqualOp.INSTANCE);
    	reg(LessThanOp.INSTANCE);
    	reg(NotEqualOp.INSTANCE);
    	reg(DivideOp.INSTANCE);
    	reg(DivOp.INSTANCE);
    	reg(IndexOp.INSTANCE);
    	reg(DollarOp.INSTANCE);
    	reg(MatchOp.INSTANCE);
    }

	public static StringCol getSupportedOperations() {
		return ColProvider.toStringCol(ops.keySet());
	}
}
