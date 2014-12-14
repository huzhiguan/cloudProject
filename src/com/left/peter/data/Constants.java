package com.left.peter.data;

public enum Constants
{
	NEW_ATOMICS(0),
	NEW_RATE(0),
	AREA_LEN(10),
	AREA_NUM(1000),
	ALL_AREAS(0);
	
	private int value;
	
	private Constants(final int value)
	{
		this.value = value;
	}
	
	public void setValue(final int value)
	{
		this.value = value;
	}
	
	public int getValue()
	{
		return value;
	}
}
