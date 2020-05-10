/**
 * Cols provide Column stores for primitive data types that allow:
 * <ul>
 *  <li>Very Fast in/greaterThan/lessThan filtering predicates where possible</li>
 *  <li>Fast scanning of all elements</li>
 *  <li>Fast bulk addAll operations</li>
 * </ul>
 * 
 * Outstanding Issues / Discussion Points:
 * <ul>
 *  <li>Compressed Columns must be possible including zippedBlocks / shrunkTypes int->short etc.</li>
 *  <li>Must be able to run predicates over full items and multiple columns</li>
 *  <li>Memory/Disk Cols must work same for querying??</li>
 *  <li>AAA</li>
 * </ul>
 * 
 * Col access is via:
 *  1. The ability to run predicates and get the locations that matches those predicates
 *  2. map(Locations) then get/set. You must call map first!
 */
@ParametersAreNonnullByDefault
package com.timestored.jdb.col;

import javax.annotation.ParametersAreNonnullByDefault;
