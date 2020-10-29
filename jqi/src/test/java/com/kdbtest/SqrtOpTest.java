package com.kdbtest;
import org.junit.Test;

public class SqrtOpTest extends OpTest{
	@Test public void sqrt0(){check("sqrt -1 0n 0 25 50","0n 0n 0 5 7.071068\n");}
}