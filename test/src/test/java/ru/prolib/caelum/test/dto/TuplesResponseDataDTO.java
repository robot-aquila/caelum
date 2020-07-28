package ru.prolib.caelum.test.dto;

import java.util.List;

import ru.prolib.caelum.core.Period;

public class TuplesResponseDataDTO {
	public String symbol;
	public Period period;
	public String format;
	public List<List<Object>> rows;
}
