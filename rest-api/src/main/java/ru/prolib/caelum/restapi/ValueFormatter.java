package ru.prolib.caelum.restapi;

import java.math.BigInteger;

import org.apache.commons.lang3.StringUtils;

public class ValueFormatter {
	
	String insertDot(String value, int decimals) {
		if ( decimals > 0 ) {
			int length = value.length();
			if ( length == decimals ) {
				return "0." + value;
			} else if ( length > decimals ) {
				int dot_pos = length - decimals;
				return value.substring(0, dot_pos) + "." + value.substring(dot_pos);
			} else {
				return "0." + StringUtils.repeat("0", decimals - length) + value;
			}
		}
		return value;
	}

	public String format(long value, int decimals) {
		return insertDot(Long.toString(value), decimals);
	}
	
	public String format(long value, BigInteger big_value, int decimals) {
		return insertDot(big_value == null ? Long.toString(value) : big_value.toString(), decimals);
	}
	
}
