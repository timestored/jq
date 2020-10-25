package com.kdbtest;
import org.junit.Test;

public class SvarOpTest extends OpTest{
	@Test public void svar0(){check("var 2 3 5 7","3.6875\n");}
	@Test public void svar1(){check("svar 2 3 5 7","4.916667\n");}
}