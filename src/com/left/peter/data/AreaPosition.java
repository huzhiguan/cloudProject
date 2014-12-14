package com.left.peter.data;

import java.util.HashMap;
import java.util.Map;

public enum AreaPosition
{
	INSIDE((byte)0),
	AROUND((byte)1),
	AROUND_AROUND((byte)2),
	NONE(Byte.MAX_VALUE);
	
	final private byte value;
	static private Map<Byte, AreaPosition> values;
	
	private AreaPosition(final byte value)
	{
		this.value = value;
	}
	
	public byte getValue()
	{
		return value;
	}
	
	static public AreaPosition valueOf(final byte value)
	{
		if (null == values)
		{
			values = new HashMap<>();
			for (final AreaPosition pos : AreaPosition.values())
			{
				values.put(pos.value, pos);
			}
		}

		if (!values.containsKey(value))
		{
			throw new IllegalArgumentException("Unknown value :" + value);
		}
		else
		{
			return values.get(value);
		}
	}
}
