package com.kdbtest;
import org.junit.Test;

public class UpperOpTest extends OpTest{
	@Test public void upper0(){check("upper\"ibm\"","\"IBM\"\n");}
	@Test public void upper0b(){check("upper (\"ibm\";\"msft\")","\"IBM\"\n\"MSFT\"\n");}
	@Test public void upper1(){check("upper`ibm","`IBM\n");}
	@Test public void upper1b(){check("upper`ibm`msft","`IBM`MSFT\n");}
}