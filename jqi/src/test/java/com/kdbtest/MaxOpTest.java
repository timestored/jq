package com.kdbtest;
import org.junit.Test;

public class MaxOpTest extends OpTest{
	@Test public void max0(){check("max 2 5 7 1 3","7\n");}
	@Test public void max1(){check("max \"genie\"","\"n\"\n");}
	@Test public void max2(){check("max 0N 5 0N 1 3","5\n");}
	@Test public void max3(){check("max 0N 0N","-0W\n");}

	@Test public void maxEmpty(){check("max ()","");}
	@Test public void maxEmptyInt(){check("max `int$()","-0Wi\n");}
}