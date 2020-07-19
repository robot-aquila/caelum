package ru.prolib.caelum.test;

import java.util.List;

public class ItemsResponseDataDTO {
	public String symbol, format;
	public List<List<Object>> rows;
	public String magic;
	public Long fromOffset;
}
