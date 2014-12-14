package com.left.peter.data;

public enum DetachRate
{
	BOND_1(4),
	BOND_2(41),
	BOND_3(446);
	
	final private int value;
	
	private DetachRate(final int value)
	{
		this.value = value;
	}
	
	public int getValue()
	{
		return value;
	}
}
