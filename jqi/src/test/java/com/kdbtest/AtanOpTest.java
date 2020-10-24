package com.kdbtest;
import org.junit.Test;

public class AtanOpTest extends OpTest{
	@Test public void atan0(){check("atan 0.5","0.4636476\n");}
	@Test public void atan1(){check("atan 42","1.546991\n");}
}