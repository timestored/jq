package com.kdbtest;
import org.junit.Test;

public class PrdOpTest extends OpTest{
	@Test public void prd0(){check("prd 7","7\n");}
	@Test public void prd1(){check("prd 2 3 5 7","210\n");}
	@Test public void prd2(){check("prd 2 3 0N 7","42\n");}
	@Test public void prd3(){check("prd (1 2 3 4;2 3 5 7)","2 6 15 28\n");}
	@Test public void prd4(){check("prd \"abc\"","'type\n");}
}