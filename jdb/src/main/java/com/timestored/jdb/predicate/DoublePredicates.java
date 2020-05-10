package com.timestored.jdb.predicate;

import com.timestored.jdb.col.DoubleCol;
import com.timestored.jdb.database.Consts;
import com.timestored.jdb.function.DoublePredicate;

import java.sql.Timestamp;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

public class DoublePredicates {

	public static DoublePredicate TRUE = new TrueDoublePredicate();
	public static DoublePredicate FALSE = new FalseDoublePredicate();
	
	public static DoublePredicate equal(double value) { return new EqualsDoublePredicate(value); }
	
	// The if's check for comparisons on the boundary that must always be true or false
	// e.g. double x = ?; x <= Double.MAX must always be true.
	
	public static DoublePredicate lessThan(double upperBound) {
		if(upperBound == Consts.Mins.DOUBLE) {
			return FALSE;
		}
		return new LessThanDoublePredicate(upperBound); 
	}
	
	public static DoublePredicate lessThanOrEqual(double upperBound) { 
		if(upperBound == Consts.Maxs.DOUBLE) {
			return TRUE;
		}
		return new LessThanOrEqualDoublePredicate(upperBound); 
	}
	
	public static DoublePredicate greaterThan(double lowerBound) { 
		if(lowerBound == Consts.Maxs.DOUBLE) {
			return FALSE;
		}
		return new GreaterThanDoublePredicate(lowerBound); 
	}
	
	public static DoublePredicate in(DoubleCol doubleCol) { 
		if(doubleCol.size() == 0) {
			return FALSE;
		}
		return new InDoublePredicate(doubleCol);
	}
	
	public static DoublePredicate greaterThanOrEqual(double lowerBound) { 
		if(lowerBound == Double.MIN_VALUE) {
			return TRUE;
		}
		return new GreaterThanOrEqualDoublePredicate(lowerBound); 
	}
	
	public static DoublePredicate between(double lowerBound, double upperBound) { 
		if(lowerBound == Consts.Mins.DOUBLE) {
			return lessThanOrEqual(upperBound);
		} else if(upperBound == Consts.Maxs.DOUBLE) {
			return greaterThanOrEqual(lowerBound);
		}
		return new BetweenDoublePredicate(lowerBound, upperBound); 
	}


	@ToString
	private static class TrueDoublePredicate implements DoublePredicate {@Override
		public boolean test(double value) { return true; }
	}

	@ToString
	private static class FalseDoublePredicate implements DoublePredicate {@Override
		public boolean test(double value) { return false; }
	}
	
	@Data
	public static class BetweenDoublePredicate implements DoublePredicate {
		private final double lowerBound;
		private final double upperBound;
		@Override public boolean test(final double value) { return value >= lowerBound && value <= upperBound; }
	}
	
	@Data
	public  static class EqualsDoublePredicate implements DoublePredicate {
		private final double v;
		@Override public boolean test(final double value) { return value == v; }
	}

	@Data
	public static class LessThanDoublePredicate implements DoublePredicate {
		private final double upperBound;
		@Override public boolean test(final double value) { return value < upperBound; }
	}
	
	@Data
	public static class LessThanOrEqualDoublePredicate implements DoublePredicate {
		private final double upperBound;
		@Override public boolean test(final double value) { return value <= upperBound; }
	}
	
	@Data
	public static class GreaterThanDoublePredicate implements DoublePredicate {
		private final double lowerBound;
		@Override public boolean test(final double value) { return value > lowerBound; }
	}
	
	@Data
	public static class GreaterThanOrEqualDoublePredicate implements DoublePredicate {
		private final double lowerBound;
		@Override public boolean test(final double value) { return value >= lowerBound; }
	}
	
	@Data
	public static class InDoublePredicate implements DoublePredicate {
		private final DoubleCol doubleCol;
		@Override public boolean test(final double value) { return doubleCol.contains(value); }
	}
}
