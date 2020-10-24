package com.kdbtest;

import java.io.IOException;
import java.util.Collections;

import com.timestored.jq.JQLauncher;
import com.timestored.jq.ops.mono.QsOp;

import static org.junit.Assert.assertEquals;

public abstract class OpTest {

    private JQLauncher cmdRunner = new JQLauncher(true, true, Collections.emptyList());
    
    
    public void check(String a, String b) throws RuntimeException {
    	checkAgainstJQ(a,b);
    }
    

    public void checkAgainstJQ(String a, String b) throws RuntimeException {
    	Object o = cmdRunner.run(a);
    	String result = QsOp.INSTANCE.asText(o);
        assertEquals("The actual result does not match expected result", b, result.replace("\r\n", "\n"));
    }

}
