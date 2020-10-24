package com.timestored.jq;

import static com.timestored.jq.ops.ParseOp.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.timestored.jdb.col.Col;
import com.timestored.jdb.col.ColProvider;
import com.timestored.jdb.col.Mapp;
import com.timestored.jdb.col.MemoryBooleanCol;
import com.timestored.jdb.col.MemoryByteCol;
import com.timestored.jdb.col.MemoryObjectCol;
import com.timestored.jdb.col.MyMapp;
import com.timestored.jdb.col.MyTbl;
import com.timestored.jdb.col.ObjectCol;
import com.timestored.jdb.col.Tbl;
import com.timestored.jdb.database.CType;
import com.timestored.jdb.database.DomainException;
import com.timestored.jdb.database.Dt;
import com.timestored.jdb.database.Time;
import com.timestored.jdb.database.Timstamp;
import com.timestored.jdb.kexception.NYIException;
import com.timestored.jdb.kexception.NamedException;
import com.timestored.jq.HelloParser.ApplyBracketContext;
import com.timestored.jq.HelloParser.ApplyContext;
import com.timestored.jq.HelloParser.AssContext;
import com.timestored.jq.HelloParser.AssignContext;
import com.timestored.jq.HelloParser.BinDoContext;
import com.timestored.jq.HelloParser.BytContext;
import com.timestored.jq.HelloParser.ExprContext;
import com.timestored.jq.HelloParser.FunctionContext;
import com.timestored.jq.HelloParser.MonthContext;
import com.timestored.jq.HelloParser.MonthListContext;
import com.timestored.jq.HelloParser.MultiExprContext;
import com.timestored.jq.HelloParser.MultiIDContext;
import com.timestored.jq.HelloParser.NestedListContext;
import com.timestored.jq.HelloParser.NumContext;
import com.timestored.jq.HelloParser.NumListContext;
import com.timestored.jq.HelloParser.ParensContext;
import com.timestored.jq.HelloParser.QueryContext;
import com.timestored.jq.HelloParser.SlashContext;
import com.timestored.jq.HelloParser.StatContext;
import com.timestored.jq.HelloParser.TableContext;
import com.timestored.jq.HelloParser.TimeContext;
import com.timestored.jq.HelloParser.TimeListContext;
import com.timestored.jq.ops.CastOp;
import com.timestored.jq.ops.Diad;
import com.timestored.jq.ops.DotOp;
import com.timestored.jq.ops.IndexOp;
import com.timestored.jq.ops.Op;
import com.timestored.jq.ops.OpRegister;
import com.timestored.jq.ops.ParseOp;
import com.timestored.jq.ops.mono.CountOp;
import com.timestored.jq.ops.mono.NiladicOp;
import com.timestored.jq.ops.mono.ProjectedOp;
import com.timestored.jq.ops.mono.QqtOp;
import com.timestored.jq.ops.mono.SystemOp;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class HelloRunner extends HelloBaseVisitor<Object> {
	private Frame env = new MyFrame();
	private Frame rootFrame = env;
	private final Context context;
	@Setter private boolean preventBuiltinOverrides = false;

	@Override
	public Object visitR(HelloParser.RContext ctx) {
		// Restore the original state always, in case previous run broke something
		if(env != rootFrame) {
			context.stderr("Warning the frame was previously misplaced");
			env = rootFrame;
		}
		List<StatContext> statements = ctx.stat();
		Object lastExpressionvalue = null;
		for (StatContext stat : statements) {
			lastExpressionvalue = visit(stat);
			if(stat.getChildCount()>1 && stat.getChild(1) instanceof AssContext) {
				lastExpressionvalue = NiladicOp.INSTANCE;
			}
		}
		return lastExpressionvalue;
	}
	
	@Override public Object visitQuery(QueryContext ctx) {
		Frame originalFrame = env;
		try {
			
			
			// Check table and setup the context
			Object tblO = visit(ctx.tbl);
			Frame tf = null;
			if(tblO instanceof Tbl) {
				tf = new TableFrame((Tbl)tblO, env);	
			} else if(QqtOp.INSTANCE.ex(tblO)) {
				tf = new TableFrame((Mapp)tblO, env); 
			} else {
				throw new TypeException("You can only select from a table");
			}
			this.env = tf;
			
			// Eval the other args
			if(ctx.wc != null) {
				List<ParseTree> wcs = ctx.wc.children;
				for(ParseTree pt : wcs) {
					System.out.println(pt.toString());
				}
			}

			return tblO;
		} finally {
			this.env = originalFrame;
		}
	}

	@Override public Object visitApplyBracket(ApplyBracketContext ctx) {
		Object a = visit(ctx.left);
		Object[] b = toArray(ctx.multiExpr());
		long argCount = b.length;
		
		if(argCount == 1 || argCount == 0) { // func[arg] alone, no parentheses
			IndexOp.INSTANCE.setContext(context);
			IndexOp.INSTANCE.setFrame(this.env);
			return IndexOp.INSTANCE.run(a, argCount == 1 ? b[0] : NiladicOp.INSTANCE);
		}
		
		if(a instanceof Op) {
			Op op = (Op) a;
			if(argCount > op.getRequiredArgumentCount()) {
				throw new RankException();
			}
			return op.run(b);
		}
		
		Object secondArg = MemoryObjectCol.of(b);
		DotOp.INSTANCE.setContext(context);
		DotOp.INSTANCE.setFrame(this.env);
		return DotOp.INSTANCE.run(a, secondArg);		
	}
	
	@Override
	public Object visitParens(ParensContext ctx) {
		return visit(ctx.expr());
	}

	@Override
	public Object visitAssign(AssignContext ctx) {
		Object b = visit(ctx.expr());
		if(ctx.ID() != null) {
			env.assign(ctx.ID().getText(), b);	
		} else {
			String s = "name";
			if(ctx.BINOP() != null) {
				s = ctx.BINOP().getText();
			} else if(ctx.MONOP() != null) {
				s = ctx.MONOP().getText();
			}
			if(preventBuiltinOverrides) {
				throw new NamedException(s);
			} else {
				env.assign(s, b);	
			}
		}
		return b;
	}

	@Override
	public Object visitTable(TableContext ctx) {
		Frame originalFrame = env;
		try {
			this.env = new MyFrame(env);
			visit(ctx.keycols);
			Mapp keyColumns = this.env.getMapp();
			this.env = new MyFrame(env);
			visit(ctx.tcols);
			Mapp tColumns = this.env.getMapp();

			if (keyColumns.size() == 0) {
				return new MyTbl(tColumns);
			}
			return new MyMapp(new MyTbl(keyColumns), new MyTbl(tColumns));
		} catch (RuntimeException re) {
		} finally {
			this.env = originalFrame;
		}
		throw new TypeException("Couldn't parse table");
	}

	@Override
	public Object visitFunction(FunctionContext ctx) {
		List<String> args = new ArrayList<>();
		if(ctx.idlist != null) {
			List<ParseTree> argList = ctx.idlist.children;
			if(argList == null) {
				args.add("");
			} else {
				for(ParseTree pt : argList) {
					if(pt.getText().trim().length() == 0) {
						throw new IllegalStateException();
					} else if(!pt.getText().trim().equals(";")) {
						args.add(pt.getText());
					}
				}
			}
		} else {
			// Discover implicit parameters properly
			args.add("x");
		}
		MyFunction func = MyFunction.builder()
				.sourcecode(ctx.getText())
				.parameters(ColProvider.toStringCol(args))
				.functionBodyContext(ctx.functionBody())
				.build(); 
		func.setFuncRunner(this::runFunction);
		return func;
	}
	
	private Object runFunction(MyFunction myFunction, Object[] args) {
		Frame originalFrame = env;
		try {
			this.env = new MyFrame(env);
			String[] argNames = myFunction.getParameters().toStringArray();
			if(argNames.length != args.length) {
				throw new RankException();
			}
			for(int i=0; i<argNames.length; i++) {
				this.env.assign(argNames[i], args[i]);	
			}
			Object lastResult = NiladicOp.INSTANCE;
			for(ParseTree pt : myFunction.getFunctionBodyContext().children) {
				lastResult = visit(pt);
			}
			//TODO Push the globals back up
			return lastResult != null ? lastResult : NiladicOp.INSTANCE;
		} catch (RuntimeException | IOException re) {
			throw new TypeException(re.toString());
		} finally {
			this.env = originalFrame;
		}
	}

	@Override public Object visitBinDo(BinDoContext ctx) {
		String cmd = ctx.BINOP().getText();
		Object f = env.get(cmd);
		if(f == null) {
			throw new UnsupportedOperationException("Dyadic Operator not found: " + cmd);
		}
		if(f instanceof Op && ((Op)f).getRequiredArgumentCount()==2) {
			Op d = (Op) f;
			d.setFrame(this.env);
			d.setContext(context);
			Object a = visit(ctx.left);
			Object b = visit(ctx.right);
			return d.run(new Object[] {a,b});
		}
		throw new TypeException();
	}

	@Override
	public Object visitApply(ApplyContext ctx) {
		Object a = visit(ctx.expr(0));
		Object b = visit(ctx.expr(1));
		IndexOp.INSTANCE.setContext(context);
		IndexOp.INSTANCE.setFrame(env);
		return IndexOp.INSTANCE.run(a, b);
	}

	@Override
	public Object visitMyid(HelloParser.MyidContext ctx) {
		String id = ctx.getText();
		if(id.startsWith(".z.")) {
			try {
				return dotz(id);
			} catch (UnknownHostException e) {
				throw new TypeException("Problem getting " + id);
			}
		}
		Object o = env.get(id);
		if(o instanceof Frame) {
			return ((Frame)o).getMapp();
		}
		return o;
	}
	
	private Object dotz(String id) throws UnknownHostException {
		switch (id) {
		case ".z.K": return 5.01;
		case ".z.c": return Runtime.getRuntime().availableProcessors();
		case ".z.h": return InetAddress.getLocalHost().getHostName();
		case ".z.a": return InetAddresses.coerceToInteger(InetAddress.getLocalHost());
		case ".z.o": return System.getProperty("os.name").toLowerCase().charAt(0) + (System.getProperty("os.arch").contains("64") ? "64" : "32");
		case ".z.q": return context.getJqLauncher().isQuiet();
		case ".z.u": return System.getProperty("user.name");
		case ".z.t": return Time.fromLocalTime(LocalTime.from(Instant.now().atZone(ZoneId.of("UTC"))));
		case ".z.T": return Time.fromLocalTime(LocalTime.from(Instant.now().atZone(ZoneId.systemDefault())));
		case ".z.d": return Dt.fromLocalDate(LocalDate.from(Instant.now().atZone(ZoneId.of("UTC"))));
		case ".z.D": return Dt.fromLocalDate(LocalDate.from(Instant.now().atZone(ZoneId.systemDefault())));
		case ".z.quote": return ColProvider.toCharacterCol(Quotes.getQuote());
		case ".z.ops": return OpRegister.getSupportedOperations();
		case ".z.handles": return context.getHandleDetails();
		
		default:
		}
		throw new IdNotFoundException(id);
	}
	
	@Override public Object visitSlash(SlashContext ctx) {
		SystemOp s = SystemOp.INSTANCE;
		s.setFrame(env);
		s.setContext(context);
		return s.run(getFullText(ctx.cmd));
	}
	
	private static String getFullText(ParserRuleContext context) {
	    if (context.start == null || context.stop == null || context.start.getStartIndex() < 0 || context.stop.getStopIndex() < 0)
	        return context.getText(); // Fallback

	    return context.start.getInputStream().getText(Interval.of(context.start.getStartIndex(), context.stop.getStopIndex()));
	}
	
	@Override
	public Object visitEmptyList(HelloParser.EmptyListContext ctx) {
		return ColProvider.emptyCol(CType.OBJECT);
	}

	@Override
	public Object visitNestedList(NestedListContext ctx) {
		Object[] vals = toArray(ctx.multiExpr());
		boolean anyNulls = false;
		for(int i=0; i<vals.length; i++) {
			if(vals[i] == null) {
				anyNulls = true;
			}
		}
		if(anyNulls) {
			return new ProjectedOp(IndexOp.INSTANCE, vals);
		}
		ObjectCol oc = MemoryObjectCol.of(vals);
		return CastOp.flattenGenericIfSameType(oc);
	}

	private Object[] toArray(MultiExprContext multiExprContext) {
		if(multiExprContext.getText().trim().equals("")) {
			return new Object[] {};
		}
		List<ParseTree> children = multiExprContext.children;
		List<Object> r = new ArrayList<>();
		for (int i = children.size() - 1; i >= 1; i--) {
			if(children.get(i).getText().trim().equals(";")) {
				r.add(null);
			} else {
				r.add(visit(children.get(i--)));
			}
		}
		r.add(children.get(0).getText().trim().equals(";") ? null : visit(children.get(0)));
		// reverse list to get array
		Object[] res = new Object[r.size()];
		for(int i=0; i<r.size(); i++) {
			res[i] = r.get(r.size() - (i+1));
		}
		return res;
	}

	@Override
	public Object visitBool(HelloParser.BoolContext ctx) {
		return b(removeTypLetter(ctx.BOOL().getText(), 'b'));
	}

	@Override
	public Object visitBoolList(HelloParser.BoolListContext ctx) {
		String t = removeTypLetter(ctx.BOOLLIST().getText(), 'b');
		MemoryBooleanCol r = new MemoryBooleanCol(t.length());
		r.setSize(t.length());
		for (int i = 0; i < r.size(); i++) {
			r.set(i, t.substring(i, i + 1).equals("1"));
		}
		return retCol(r);
	}

	private static Col retCol(Col vals) { vals.setSorted(false); return vals; }
	
	@Override public Object visitNum(NumContext ctx) {
		String t = ctx.getText().trim();
		char typeChar = getTypeChar(t);
		if(t.charAt(t.length()-1) == typeChar) {
			t = t.substring(0, t.length()-1);
		}
		return ParseOp.PARSE.parse(typeChar, t, false);
	}

	private static char getTypeChar(String t) {
		 
		char lastChar = t.charAt(t.length()-1);
		char pChar = t.length() >= 2 ? t.charAt(t.length()-2) : ' ';
		char ppChar = t.length() >= 3 ? t.charAt(t.length()-3) : ' ';
		// "0N"  "0W"   "0Nn"   "0n" "0Wn"
		// Try to use last letter except being careful of double nulls.
		CType cType = CType.getType(lastChar);
		if(ppChar == ' ' && pChar == '0' && lastChar == 'n') {
			cType = CType.DOUBLE;
		}
		// Else assume defaults
		if(cType == null) {
			if(t.contains(".") || t.contains("w") || t.contains("n")) {
				cType = CType.DOUBLE;	
			} else {
				cType = CType.LONG;
			}
		}
		char typeChar = cType.getCharacterCode();
		return typeChar;
	}
	
	@Override public Object visitNumList(NumListContext ctx) {
		String t = ctx.getText().trim();
		char typeChar = getTypeChar(t);
		if(t.charAt(t.length()-1) == typeChar) {
			t = t.substring(0, t.length()-1);
		}
		return ParseOp.PARSE.parse(typeChar, t.split(" ", -1), false);
	}

	private static String removeTypLetter(String dtString, char typLetter) {
		String t = dtString.trim().toLowerCase();
		if (t.charAt(t.length() - 1)== typLetter) {
			t = t.substring(0, t.length() - 1);
		}
		return t;
	}

	@Override public Object visitSymbol(HelloParser.SymbolContext ctx) {
		return ctx.getText().substring(1);
	}

	@Override public Object visitSymbolList(HelloParser.SymbolListContext ctx) {
		String t = ctx.getText().substring(1);
		List<String> ls = Arrays.asList(t.split("`", -1));
		return retCol(ColProvider.toStringCol(ls)); 
	}

	@Override public Object visitChar(HelloParser.CharContext ctx) {
		return ctx.getText().charAt(1);
	}

	@Override public Object visitCharList(HelloParser.CharListContext ctx) {
		String t = ctx.getText();
		return retCol(ColProvider.toCharacterCol(t.substring(1, t.length()-1)));
	}
	
	@Override public Object visitByt(BytContext ctx) {
		String t = ctx.getText().substring(2);
		byte[] ba = ParseOp.x(t);
		return ba.length == 1 ? ba[0] : new MemoryByteCol(ba);
	}
	
	
	@Override public Object visitTime(TimeContext ctx) {
		String t = ctx.TIME().getText().trim().toLowerCase();
		if(t.length() < 2) {
			throw new IllegalArgumentException("couldn't recognise time: " + t);
		}
		char typ = t.charAt(t.length()-1);
		if("nuvt".contains(""+typ)) {
			t = t.substring(0, t.length()-1);
		} else if(t.contains("d")){
			typ = 'n';
		} else {
			typ = getTyp(t.length());
		}
		switch(typ) {
			case 'n': return n(t);
			case 'u': return u(t);
			case 'v': return v(t);
			case 't': return t(t);
		}
		throw new TypeException("can't parse time");
	}

	private char getTyp(int tlength) {
		return tlength == 5 ? 'u' : tlength == 8 ? 'v' 
				: tlength > 8 && tlength <= 13 ? 't' 
				: tlength > 13 ? 'n'
				: ' ';
	}
	
	@Override public Object visitTimeList(TimeListContext ctx) {
		String t = ctx.TIMELIST().getText().trim().toLowerCase();
		char typ = t.charAt(t.length()-1);
		if("nuvt".contains(""+typ)) {
			t = t.substring(0, t.length()-1);
		} else {
			int idx = 0;
			String order = " uvtn";
			String[] defs = t.split(" ");
			Preconditions.checkArgument(defs.length>1);
			for(String d : defs) {
				int latestIdx = order.indexOf(getTyp(d.length()));
				if(latestIdx > idx) {
					idx = latestIdx;
				}
			}
			typ = order.charAt(idx);
		}
		
		switch(typ) {
			case 'n': return n(t.split(" ", -1));
			case 'u': return u(t.split(" ", -1));
			case 'v': return v(t.split(" ", -1));
			case 't': return t(t.split(" ", -1));
		}
		throw new DomainException("Couldn't interpret: " + t);
	}
	
	@Override public Object visitMonth(MonthContext ctx) {
		return m(removeTypLetter(ctx.MONTH().getText(),'m'));
	}
	
	@Override public Object visitMonthList(MonthListContext ctx) {
		return m(sp(ctx.MONTHLIST().getText(),'m'));
	}

	@Override public Object visitDate(HelloParser.DateContext ctx) {
		String t = ctx.DATE().getText();
		char typ = t.charAt(t.length()-1);
		if("dp".contains(""+typ)) {
			t = t.substring(0, t.length()-1);
		} else {
			typ = t.contains("D") ? 'p' : 'd';
		}
		switch(typ) {
			case 'p': return p(t);
			case 'd': return d(t);
		}
		throw new TypeException("can't parse date");
	}

	@Override public Object visitDateList(HelloParser.DateListContext ctx) {
		String t = ctx.DATELIST().getText();
		char typ = t.charAt(t.length()-1);
		if("dp".contains(""+typ)) {
			t = t.substring(0, t.length()-1);
		} else {
			typ = t.contains("D") ? 'p' : 'd';
		}
		switch(typ) {
			case 'p': return p(t.split(" ", -1));
			case 'd': return d(t.split(" ", -1));
		}
		throw new TypeException("can't parse datelist");
	}

	private String[] sp(String t, char typeLetter) {
		t = t.trim().toLowerCase();
		t = removeTypLetter(t,typeLetter);
		return t.split(" ", -1);
	}
	
}
