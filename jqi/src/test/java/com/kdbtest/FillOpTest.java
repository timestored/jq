package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

public class FillOpTest extends OpTest{
	@Test public void fill0(){check("0^1 2 3 0N","1 2 3 0\n");}
	@Test public void fill1(){check("100^1 2 -5 0N 10 0N","1 2 -5 100 10 100\n");}
	@Test public void fill2(){check("1.0^1.2 -4.5 0n 0n 15","1.2 -4.5 1 1 15\n");}
	@Test public void fill3(){check("`nobody^`tom`dick``harry","`tom`dick`nobody`harry\n");}
	@Test public void fill4(){check("1 2 3 4 5^6 0N 8 9 0N","6 2 8 9 5\n");}
	@Test public void fill5(){check("a:11.0 2.1 3.1 0n 4.5 0n;type a","9h\n");}
	@Ignore
	@Test public void fill6(){check("10^a","11 2.1 3.1 10 4.5 10\n");}
	@Ignore
	@Test public void fill7(){check("type 10^a","9h\n");}
	@Ignore
	@Test public void fill8(){check("(`a`b`c!1 2 3)^`b`c!0N 30","a| 1\nb| 2\nc| 30\n");}
}