package com.left.peter.atomic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.left.peter.data.AreaNumber;
import com.left.peter.data.Constants;
import com.left.peter.data.Direction;
import com.left.peter.data.Position;

public class AtomicReducer extends Reducer<IntWritable, AtomicWritable, NullWritable, Text> 
{
	private static final AreaNumber HELPER = new AreaNumber();
	private static final Random RANDOM = new Random();
	private List<Position> positions = new ArrayList<Position>();
	
	private Set<Direction> getNeighorDirections(final AtomicWritable atomic, final Set<Position> set)
	{
		final Set<Direction> dicts = new HashSet<>();
		
		if (set.contains(new Position(atomic.getPos().getX(), HELPER.adjust(atomic.getPos().getY() + 1))))
		{
			dicts.add(Direction.RIGHT);
		}
		
		if (set.contains(new Position(atomic.getPos().getX(), HELPER.adjust(atomic.getPos().getY() - 1))))
		{
			dicts.add(Direction.LEFT);
		}
		
		if (set.contains(new Position(atomic.getPos().getX() - 1, HELPER.adjust(atomic.getPos().getY()))))
		{
			dicts.add(Direction.UP);
		}
		
		if (set.contains(new Position(atomic.getPos().getX() + 1, HELPER.adjust(atomic.getPos().getY()))))
		{
			dicts.add(Direction.DOWN);
		}
		
		return dicts;
	}
	
	private Direction getDirection(final AtomicWritable atomic, final Set<Direction> dicts)
	{
		Direction dict = Direction.STAY;
		
		final boolean move;
		
		switch (dicts.size())
		{
		case 0:
			move = true;
			break;
			
		case 1:
			move = atomic.getDetach1Bond();
			break;
			
		case 2:
			move = atomic.getDetach2Bond();
			break;
			
		case 3:
			move = atomic.getDetach3Bond();
			break;
			
		default:
			move = false;
			break;
		}
		
		if (move)
		{
			for (final Direction optional : atomic.getDirections())
			{
				if (!dicts.contains(optional))
				{
					dict = optional;
					break;
				}
			}
		}
		
		return dict;
	}
	
	private void insideMove(final AtomicWritable atomic, final Direction dict, final Set<Position> newPos, final int area, final Context context) throws IOException, InterruptedException
	{
		boolean bStay = true;
		if (Direction.STAY != dict)
		{
			final Position next = getNextPos(atomic, dict);
			
			bStay = false;
			// Still in the area.
			if (inArea(area, next))
			{
				// Others haven moved into the position.
				if (newPos.add(next))
				{
					context.write(NullWritable.get(), new Text(next.toString()));
				}
				else
				{
					bStay = true;
				}
			}
		}
		
		if (bStay && inArea(area, atomic.getPos()) && newPos.add(atomic.getPos()))
		{
			context.write(NullWritable.get(), new Text(atomic.getPos().toString()));
		}
	}
	
	private void aroundMove(final AtomicWritable atomic, final Direction dict, final Set<Position> newPos, final int area, final Context context) throws IOException, InterruptedException
	{
		if (Direction.STAY != dict)
		{
			final Position next = getNextPos(atomic, dict);
			
			if (inArea(area, next) && newPos.add(next))
			{
				context.write(NullWritable.get(), new Text(next.toString()));			
			}			
		}
	}
	
	private Position getNextPos(final AtomicWritable atomic, final Direction dict)
	{
		final Position pos;
		switch (dict)
		{
		case RIGHT:
			pos = new Position(atomic.getPos().getX(), HELPER.adjust(atomic.getPos().getY() + 1));
			break;
		case UP:
			pos = new Position(HELPER.adjust(atomic.getPos().getX() - 1), atomic.getPos().getY());
			break;
		case DOWN:
			pos = new Position(HELPER.adjust(atomic.getPos().getX() + 1), atomic.getPos().getY());
			break;
		case STAY:
			pos = atomic.getPos();
			break;
		case LEFT:
			pos = new Position(atomic.getPos().getX(), HELPER.adjust(atomic.getPos().getY() - 1));
			break;
		default:
			throw new IllegalArgumentException("Unknown direction " + dict);
		}
		
		return pos;
	}
	
	private boolean inArea(final int area, final Position pos)
	{
		return area == HELPER.areaNumber(HELPER.getArea(pos));
	}	
	
	@Override
	public void reduce(final IntWritable key, final Iterable<AtomicWritable> values, final Context context)
			throws IOException, InterruptedException
	{
		final List<AtomicWritable> insides = new ArrayList<>();
		final List<AtomicWritable> arounds = new ArrayList<>();
		final int area = key.get();
		final Set<Position> pos = new HashSet<>();
		final Set<Position> newPos = new HashSet<>();
		
		for (final AtomicWritable item : values)
		{
			final AtomicWritable atomic = new AtomicWritable(item);
			if (pos.add(atomic.getPos()))
			{
				switch (atomic.getAreaPosition())
				{
				case AROUND:
					arounds.add(atomic);
					break;
				case INSIDE:
					insides.add(atomic);
					break;
				case AROUND_AROUND:
					break;
				default:
					throw new IllegalArgumentException("Unknown type :%s" + atomic.getAreaPosition());
				}
			}
			else
			{
				throw new IllegalArgumentException("Repeat atomic :%s" + atomic);
			}
		}
		
		// Deal with boundary first;
		for (final AtomicWritable atomic : arounds)
		{
			final Set<Direction> dicts = getNeighorDirections(atomic, pos);
			final Direction dict = getDirection(atomic, dicts);
			aroundMove(atomic, dict, newPos, area, context);
		}
		
		// Deal with inside next.
		for (final AtomicWritable atomic : insides)
		{
			final Set<Direction> dicts = getNeighorDirections(atomic, pos);
			final Direction dict = getDirection(atomic, dicts);
			insideMove(atomic, dict, newPos, area, context);
		}
		
		// Create new atomics.
		final Position areaPos = HELPER.numberToArea(area);
		
		final Random rand = new Random(System.nanoTime());
		
		if (Constants.NEW_RATE.getValue() > 0 && rand.nextInt(100) < Constants.NEW_RATE.getValue())
		{
			for (int i = 0; i < Constants.NEW_ATOMICS.getValue(); ++i)
			{
				final Position random = positions.get(RANDOM.nextInt(positions.size()));
				final Position randomPos = new Position(areaPos.getX() * Constants.AREA_LEN.getValue() + random.getX(),
						areaPos.getY() * Constants.AREA_LEN.getValue() + random.getY());
				if (!pos.contains(randomPos) && newPos.add(randomPos))
				{
					context.write(NullWritable.get(), new Text(randomPos.toString()));
				}
			
			}
		}
	}

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		
		final Configuration conf = context.getConfiguration();
		Constants.AREA_LEN.setValue(conf.getInt(Constants.AREA_LEN.toString(), Constants.AREA_LEN.getValue()));
		Constants.AREA_NUM.setValue(conf.getInt(Constants.AREA_NUM.toString(), Constants.AREA_NUM.getValue()));
		Constants.NEW_ATOMICS.setValue(conf.getInt(Constants.NEW_ATOMICS.toString(), Constants.NEW_ATOMICS.getValue()));
		Constants.ALL_AREAS.setValue(conf.getInt(Constants.ALL_AREAS.toString(), Constants.ALL_AREAS.getValue()));
		Constants.NEW_RATE.setValue(conf.getInt(Constants.NEW_RATE.toString(), Constants.NEW_RATE.getValue()));
		
		for (int i = 0; i < Constants.AREA_LEN.getValue(); ++i)
		{
			for (int j = 0; j < Constants.AREA_LEN.getValue(); ++j)
			{
				positions.add(new Position(i, j));
			}
		}
	}
}
