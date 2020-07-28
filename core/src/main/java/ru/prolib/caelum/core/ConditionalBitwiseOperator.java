package ru.prolib.caelum.core;

import java.util.function.IntBinaryOperator;

public interface ConditionalBitwiseOperator extends IntBinaryOperator {
	boolean applied();
}