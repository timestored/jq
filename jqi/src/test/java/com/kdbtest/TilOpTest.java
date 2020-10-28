package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

public class TilOpTest extends OpTest{
	@Test public void til0(){check("til 0","`long$()\n");}
	@Test public void til1(){check("til 1b",",0\n");}
	@Test public void til2(){check("til 5","0 1 2 3 4\n");}
	@Ignore
	@Test public void til3(){check("til 5f","type");}
}