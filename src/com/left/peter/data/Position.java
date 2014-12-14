package com.left.peter.data;

public class Position
{
	private final int x;
	private final int y;
	
	public Position(final int x, final int y)
	{
		this.x = x;
		this.y = y;
	}
	
	public Position(final Position o)
	{
		this(o.x, o.y);
	}
	
	@Override
	public boolean equals(final Object pos)
	{
		if (pos == this)
		{
			return true;
		}
		else if (pos instanceof Position)
		{
			final Position o = (Position) pos;
			return x == o.x && y == o.y;
		}
		else
		{
			return false;
		}
	}
	
	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 17 * hash + x;
		hash = 17 * hash + y;
		return hash;
		
	}
	
	@Override
	public String toString()
	{
		return String.format("%d,%d", x, y);
	}
	
	public int getX()
	{
		return x;
	}
	
	public int getY()
	{
		return y;
	}
}
