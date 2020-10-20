package com.timestored.jq;

import java.util.List;

import org.antlr.v4.runtime.tree.ParseTree;

import com.timestored.jdb.col.StringCol;
import com.timestored.jq.HelloParser.FunctionBodyContext;
import com.timestored.jq.ops.Op;
import com.timestored.jq.ops.mono.ProjectedOp;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder
public class MyFunction implements Op {

	@Getter @Setter protected Frame frame;
	@Getter @Setter protected Context context;
	@Getter @Setter public Object[] args;
	
	private final StringCol parameters;
	private final StringCol locals;
	private final StringCol globals;
	private final String fullname;
	private final String filename;
	private final String sourcecode;
	private final int lineNumber;
	private final FunctionBodyContext functionBodyContext;
	private FuncRunner funcRunner;
	
	public String name() { return sourcecode; }
	public short typeNum() { return 100; }
	
	public Object run(Object[] args) {
    	if(args.length > parameters.size()) {
    		throw new RankException();
    	}
    	boolean canRun = args.length == parameters.size();
    	if(canRun) {
    		for(int i=0; i<args.length; i++) {
    			if(args[i] == null) {
    				canRun = false;
					break;
    			}
    		}
    	}
    	return canRun ? funcRunner.run(this, args) : new ProjectedOp(this, args);
	}
	
	public int getNumberOfArgumentsSupplied() { return 0; }
	@Override public int getRequiredArgumentCount() { return parameters.size(); }

	@Override public String toString() { return name(); }
	
	public static interface FuncRunner {
		public Object run(MyFunction myFunction, Object[] args);
	}
}
