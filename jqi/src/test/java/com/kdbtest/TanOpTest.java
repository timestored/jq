package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

public class TanOpTest extends OpTest{
	@Ignore
	@Test public void tan0(){check(MdevOpTest.PREC + "tan 0 0.5 1 1.5707963 2 0w","0 0.5463025 1.557408 3.732054e+007 -2.18504 0n\n");}
}