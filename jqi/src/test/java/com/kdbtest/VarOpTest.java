package com.kdbtest;
import org.junit.Test;

public class VarOpTest extends OpTest{
	@Test public void var0(){check("var 2 3 5 7","3.6875\n");}
	@Test public void var1(){check("var 2 3 5 0n 7","3.6875\n");}
}