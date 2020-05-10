package com.timestored.jdb.predicate;

/**
 * Allows getting Typed predicates hopefully in a sane way given the large type space.
 * There are a number of type issues:
 * 1. The type of the argument when creating the predicate e.g. equal(1), equal(1.0)
 * 2. The type of the values being tested, e.g. IntPredicate/FloatPredicate
 * This class aims to allow deferred construction of common predicates in a nice way.
 */
public class JPredicates {

	public static PredicateFactory equal(Object value) {  
		return new PF1(SuperPredicates.equal, CastConvertStrategy.INSTANCE, value); 
	}

	public static PredicateFactory lessThan(Object value) {  
		return new PF1(SuperPredicates.lessThan, CastConvertStrategy.INSTANCE, value); 
	}

	public static PredicateFactory lessThanOrEqual(Object value) {  
		return new PF1(SuperPredicates.lessThanOrEqual, CastConvertStrategy.INSTANCE, value); 
	}

	public static PredicateFactory greaterThan(Object value) {  
		return new PF1(SuperPredicates.greaterThan, CastConvertStrategy.INSTANCE, value); 
	}

	public static PredicateFactory greaterThanOrEqual(Object value) {  
		return new PF1(SuperPredicates.greaterThanOrEqual, CastConvertStrategy.INSTANCE, value); 
	}


	public static PredicateFactory between(Object lowerBound, Object upperBound) {  
		return new PF2(SuperPredicates.between, CastConvertStrategy.INSTANCE, CastConvertStrategy.INSTANCE, lowerBound, upperBound); 
	}

}
