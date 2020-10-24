package com.kdbtest;
import org.junit.Test;

public class LastOpTest extends OpTest{
	@Test public void last0(){check("last til 10","9\n");}
	@Test public void last1(){check("last `a`b`c!1 2 3","3\n");}
	@Test public void last2(){check("last 42","42\n");}
}