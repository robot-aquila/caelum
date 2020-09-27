package ru.prolib.caelum.lib;

import java.util.function.IntBinaryOperator;

public interface ConditionalBitwiseOperator extends IntBinaryOperator {
	boolean applied();
}