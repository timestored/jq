package com.timestored.jq;

import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.base.Preconditions;
import com.timestored.jdb.col.IntegerCol;
import com.timestored.jdb.col.MemoryIntegerCol;
import com.timestored.jq.ops.mono.QsOp;

import lombok.Getter;
import lombok.Setter;

public class Context {

    @Getter private Path currentDir;
    @Getter private JQLauncher jqLauncher;
    
    public Context(Path currentDir, JQLauncher jqLauncher) {
    	this.currentDir = Preconditions.checkNotNull(currentDir);
    	this.jqLauncher = Preconditions.checkNotNull(jqLauncher);
    	Preconditions.checkArgument(Files.isDirectory(currentDir));
	}

	public void setCurrentDir(Path path) {
		if(!Files.isDirectory(path)) {
			throw new TypeException("Can't change directory to: " + path.toString() + " as it's not a directory");
		}
		this.currentDir = path;
	}

	public void stdout(String s) { System.out.print(s); }
	public void stderr(String s) { System.err.print(s); }
	
	public int getPrecision() {
		return QsOp.INSTANCE.getPrecision();
	}
	
	public void setPrecision(int precision) {
		QsOp.INSTANCE.setPrecision(precision);
	}

	public int getConsoleRows() {
		return QsOp.INSTANCE.getRows();
	}
	
	public int getConsoleColumns() {
		return QsOp.INSTANCE.getColumns();
	}
	
	public IntegerCol getConsole() {
		return new MemoryIntegerCol(getConsoleRows(), getConsoleColumns());
	}
	
	public void setConsole(int rows, int columns) {
		QsOp.INSTANCE.setConsole(rows, columns);
	}

}
