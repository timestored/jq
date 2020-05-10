package com.timestored.jdb.predicate;

import com.timestored.jdb.function.StringPredicate;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

public class StringPredicates {

	public static StringPredicate TRUE = new TrueStringPredicate();
	public static StringPredicate FALSE = new FalseStringPredicate();
	
	public static StringPredicate equal(String value) { return new EqualsStringPredicate(value); }
	
	// The if's check for comparisons on the boundary that must always be true or false
	// e.g. String x = ?; x <= String.MAX must always be true.

	
	public static StringPredicate between(String lowerBound, String upperBound) { 
		return new BetweenStringPredicate(lowerBound, upperBound); 
	}
	
	public static StringPredicate lessThan(String upperBound) {
		return new LessThanStringPredicate(upperBound); 
	}
	
	public static StringPredicate lessThanOrEqual(String upperBound) { 
		return new LessThanOrEqualStringPredicate(upperBound); 
	}
	
	public static StringPredicate greaterThan(String lowerBound) { 
		return new GreaterThanStringPredicate(lowerBound); 
	}
	
	public static StringPredicate greaterThanOrEqual(String lowerBound) { 
		return new GreaterThanOrEqualStringPredicate(lowerBound); 
	}


	@Data
	public static class TrueStringPredicate implements StringPredicate {@Override
		public boolean test(String value) { return true; }
	}

	@Data
	public static class FalseStringPredicate implements StringPredicate {@Override
		public boolean test(String value) { return false; }
	}
	
	@Data
	public static class EqualsStringPredicate implements StringPredicate {
		private final String v;
		@Override public boolean test(final String value) { return value.equals(v); }
	}
	
	@Data
	public static class BetweenStringPredicate implements StringPredicate {
		private final String lowerBound;
		private final String upperBound;
		@Override public boolean test(final String value) { 
			return (value.compareTo(upperBound) <= 0) && (value.compareTo(lowerBound) >= 0); 
		}
	}

	@Data
	public static class LessThanStringPredicate implements StringPredicate {
		private final String upperBound;
		@Override public boolean test(final String value) { return value.compareTo(upperBound) < 0; }
	}
	
	@Data
	public static class LessThanOrEqualStringPredicate implements StringPredicate {
		private final String upperBound;
		@Override public boolean test(final String value) { return value.compareTo(upperBound) <= 0; }
	}
	
	@Data
	public static class GreaterThanStringPredicate implements StringPredicate {
		private final String lowerBound;
		@Override public boolean test(final String value) { return value.compareTo(lowerBound) > 0; }
	}
	
	@Data
	public static class GreaterThanOrEqualStringPredicate implements StringPredicate {
		private final String lowerBound;
		@Override public boolean test(final String value) { return value.compareTo(lowerBound) >= 0; }
	}
}
