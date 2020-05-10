package com.timestored.jdb.misc;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

public class StopperWatch {
	
	private Stopwatch stopwatch;

	private StopperWatch() { stopwatch = Stopwatch.createStarted(); }

	public static StopperWatch createStarted() { return new StopperWatch(); }

	public long displayAndResetStopwatch(String description) {
		long millis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
		System.out.println(description + " time taken: " + millis);
		stopwatch.reset().start();
		return millis;
	}
}