package com.timestored.jq.ops;

import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.database.SpecialValues;
import com.timestored.jq.ops.mono.KRunnerDoubleMapBase;

public class AvgsOp extends KRunnerDoubleMapBase {
    public static final AvgsOp INSTANCE = new AvgsOp();
	@Override public String name() { return "avgs"; }

    public DoubleCol placeCalcInto(DoubleCol dc) {
        int count = 0;
        double sum = 0;
        double avgs = SpecialValues.nf;
        for(int i=0; i<dc.size(); i++) {
        	double v = dc.get(i);
            if(!Double.isNaN(v)) {
                sum += v;
                count++;
                avgs = sum/count;
            }
            dc.set(i, avgs);
        };
        return dc;
    }

}
