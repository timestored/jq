package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

public class AvgOpTest extends OpTest{
	@Test public void avg0(){check("avg 1 2 3","2f\n");}
	@Test public void avg1(){check("avg 1 0n 2 3","2f\n");}
	@Ignore
	@Test public void avg2(){check("avg (1 2;0N 4)","0n 3\n");}
	@Test public void avg3(){check("avg 1.0 0w","0w\n");}
	@Test public void avg4(){check("avg -0w 0w","0n\n");}
	@Ignore
	@Test public void avgEmptyGeneric(){check("avg ()","0n");}
}