package com.kdbtest;
import org.junit.Test;

public class SdevOpTest extends OpTest{
	@Test public void sdev0(){check("sdev 10 343 232 55","155.1322\n");}
}