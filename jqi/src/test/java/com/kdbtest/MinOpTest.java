package com.kdbtest;
import org.junit.Test;

public class MinOpTest extends OpTest{
	@Test public void min0(){check("min 2 5 7 1 3","1\n");}
	@Test public void min1(){check("min \"genie\"","\"e\"\n");}
	@Test public void min2(){check("min 0N 5 0N 1 3","1\n");}
	@Test public void min3(){check("min 0N 0N","0W\n");}
}