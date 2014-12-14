package com.left.peter.data;

import java.util.HashMap;
import java.util.Map;

public enum Direction
{
	UP((byte)0),
	DOWN((byte)1),
	LEFT((byte)2),
	RIGHT((byte)3),
	STAY(Byte.MAX_VALUE);
	
	private final byte value;
	static private Map<Byte, Direction> values;
	
	private Direction(final byte value)
	{
		this.value = value;
	}
	
	public byte getValue()
	{
		return value;
	}
	
	static public Direction valueOf(final byte value)
	{
		if (null == values)
		{
			values = new HashMap<>();
			for (final Direction dict : Direction.values())
			{
				values.put(dict.value, dict);
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
