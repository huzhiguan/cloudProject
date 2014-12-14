package com.left.peter.atomic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.left.peter.data.AreaPosition;
import com.left.peter.data.Direction;
import com.left.peter.data.Position;

/**
 * Writable for rating data, it's used for data transferring between mapper and reducer.
 *
 */
public class AtomicWritable implements Writable
{
	/** coordinate */
	private Position pos;
	/** Direction */
	private List<Direction> dicts;
	
	private AreaPosition area;
	/** true the atomic will detach when it's 1 bond */
	private boolean detach1Bond;
	/** true the atomic will detach when it's 2 bond */
	private boolean detach2Bond;
	/** true the atomic will detach when it's 3 bond */
	private boolean detach3Bond;
	
	@Override
	public String toString()
	{
		return String.format("(%s) {%s} [%s][%b, %b, %b]", pos, dicts.toString(), area, 
				detach1Bond, detach2Bond, detach3Bond);
	}
	
	@Override
	public boolean equals(final Object o)
	{
		final boolean equal;
		if (o instanceof AtomicWritable)
		{
			final AtomicWritable other = (AtomicWritable) o;
			equal = pos.equals(other.pos);
		}
		else
		{
			equal = false;
		}
		
		return equal;
	}
	
	@Override
	public int hashCode()
	{
		return pos.hashCode();
	}
	
	public AtomicWritable()
	{
		this(new Position(0, 0), Collections.<Direction>emptyList(), AreaPosition.NONE, false, false, false);
	}
	
	public AtomicWritable(final Position pos, final List<Direction> dicts, final AreaPosition area,
			boolean detach1Bond, boolean detach2Bond, boolean detach3Bond)
	{
		this.pos = pos;
		this.dicts = new ArrayList<>(dicts);
		this.area = area;
		this.detach1Bond = detach1Bond;
		this.detach2Bond = detach2Bond;
		this.detach3Bond = detach3Bond;
	}
	
	public AtomicWritable(final AtomicWritable o)
	{
		this(new Position(o.pos), o.dicts, o.area, o.detach1Bond, o.detach2Bond, o.detach3Bond);
	}
	
	public Position getPos()
	{
		return pos;
	}
	
	public List<Direction> getDirections()
	{
		return dicts;
	}
	
	public void setAreaPosition(final AreaPosition area)
	{
		this.area = area;
	}
	
	public AreaPosition getAreaPosition()
	{
		return area;
	}
	
	public boolean getDetach1Bond()
	{
		return detach1Bond;
	}
	
	public boolean getDetach2Bond()
	{
		return detach2Bond;
	}
	
	public boolean getDetach3Bond()
	{
		return detach3Bond;
	}
	
	@Override
	public void readFields(final DataInput in) throws IOException
	{
		pos = new Position(in.readInt(), in.readInt());
		final int nums = in.readInt();
		dicts = new ArrayList<>();
		for (int i = 0; i < nums; ++i)
		{
			dicts.add(Direction.valueOf(in.readByte()));
		}
		area = AreaPosition.valueOf(in.readByte());
		detach1Bond = in.readBoolean();
		detach2Bond = in.readBoolean();
		detach3Bond = in.readBoolean();
	}
	
	@Override
	public void write(final DataOutput out) throws IOException
	{
		out.writeInt(pos.getX());
		out.writeInt(pos.getY());
		out.writeInt(dicts.size());
		for (final Direction dict : dicts)
		{
			out.writeByte(dict.getValue());
		}
		out.writeByte(area.getValue());
		out.writeBoolean(detach1Bond);
		out.writeBoolean(detach2Bond);
		out.writeBoolean(detach3Bond);
	}
}
