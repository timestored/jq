package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

public class DistinctOpTest extends OpTest{
	@Test public void distinct0(){check("distinct 2 3 7 3 5 3","2 3 7 5\n");}
	@Test public void distinct1(){check("distinct flip `a`b`c!(1 2 1;2 3 2;\"aba\")","a b c\n-----\n1 2 a\n2 3 b\n");}
    @Ignore
	@Test public void distinct2(){check("system \"P 14\";distinct 2 + 0f,10 xexp -13","2 2.0000000000001\n");}
}