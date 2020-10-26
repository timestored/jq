package com.kdbtest;
import org.junit.Test;

public class LowerOpTest extends OpTest{
	@Test public void lower0(){check("lower\"IBM\"","\"ibm\"\n");}
	@Test public void lower1(){check("lower`IBM","`ibm\n");}
}