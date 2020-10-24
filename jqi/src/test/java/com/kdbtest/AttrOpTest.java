package com.kdbtest;
import org.junit.Test;

public class AttrOpTest extends OpTest{
	@Test public void attr0(){check("attr 1 3 4","`\n");}
	@Test public void attr1(){check("attr asc 1 3 4","`s\n");}
}