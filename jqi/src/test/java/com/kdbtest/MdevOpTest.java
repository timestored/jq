package com.kdbtest;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class MdevOpTest extends OpTest{
	final static String PREC = "system \"P 7\";";
	@Test public void mdev0(){check(PREC + "2 mdev 1 2 3 5 7 10","0 0.5 0.5 1 1 1.5\n");}
	@Test public void mdev1(){check(PREC + "5 mdev 1 2 3 5 7 10","0 0.5 0.8164966 1.47902 2.154066 2.87054\n");}
	@Test public void mdev2(){check(PREC + "5 mdev 0N 2 0N 5 7 0N","0n 0 0 1.5 2.054805 2.054805\n");}
}