package com.left.peter.data;

public class AreaNumber
{
	public int areaNumber(final Position pos)
	{	
		return pos.getX() * Constants.AREA_NUM.getValue() + pos.getY();
	}
	
	public Position numberToArea(final int num)
	{
		return new Position(num / Constants.AREA_NUM.getValue(), num % Constants.AREA_NUM.getValue());
	}
	
	/**
	 * Get area position.
	 * @param pos position of the atomic.
	 * @return position of the area.
	 */
	public Position getArea(final Position pos)
	{
		return new Position(pos.getX() / Constants.AREA_LEN.getValue(), 
				pos.getY() / Constants.AREA_LEN.getValue());
	}
	
	public int adjust(final int coor)
	{
		return (coor >= 0) ? coor % (Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue()) : (coor + Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue());
//		return (coor % (Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue()));
	}
}
