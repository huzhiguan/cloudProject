package com.left.peter.atomic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.left.peter.data.AreaNumber;
import com.left.peter.data.AreaPosition;
import com.left.peter.data.Constants;
import com.left.peter.data.DetachRate;
import com.left.peter.data.Direction;
import com.left.peter.data.Position;

public class AtomicMapper extends Mapper<LongWritable, Text, IntWritable, AtomicWritable> 
{
	private static final String SPLIT_STRING = ",";
	private static final int TOTAL_COUNT = 2;
	private static final int X_POSITION = 0;
	private static final int Y_POSITION = 1;
	private static final AreaNumber HELPER = new AreaNumber();
	private static final Direction[] DIRS = {Direction.LEFT, Direction.UP, Direction.RIGHT, Direction.DOWN};
	
	private List<Direction> random()
	{
		List<Direction> rand = new ArrayList<>(Arrays.asList(DIRS));
		Collections.shuffle(rand, new Random(System.nanoTime()));
		return rand;
	}
	
	private AtomicWritable createAtomic(final Position node, final AreaPosition pos)
	{
		final Random rand = new Random(System.nanoTime());
		return new AtomicWritable(node, random(), pos, 0 == rand.nextInt(DetachRate.BOND_1.getValue()), 
				0 == rand.nextInt(DetachRate.BOND_2.getValue()), 0 == rand.nextInt(DetachRate.BOND_3.getValue()));
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
	}
	
	@Override
	protected void map(final LongWritable key, final Text value, final Context context)
			throws IOException, InterruptedException
	{
		final String[] values = value.toString().split(SPLIT_STRING);
		
		if (Constants.ALL_AREAS.getValue() == 1)
		{
			Constants.ALL_AREAS.setValue(0);
			final AtomicWritable atomic = createAtomic(new Position(-1, -1), AreaPosition.AROUND_AROUND);
			for (int i = 0; i < Constants.AREA_NUM.getValue() * Constants.AREA_NUM.getValue(); ++i)
			{
					context.write(new IntWritable(i), atomic);
			}
		}
		
		if (values.length == TOTAL_COUNT)
		{
			final int x = Integer.parseInt(values[X_POSITION]);
			final int y = Integer.parseInt(values[Y_POSITION]);
			
			final Position node = new Position(x, y);
			final Position area = HELPER.getArea(node);
			final AtomicWritable atomic = createAtomic(node, AreaPosition.INSIDE);
			final AtomicWritable aroundAtomic = new AtomicWritable(atomic);
			aroundAtomic.setAreaPosition(AreaPosition.AROUND);
			final AtomicWritable aroundAAtomic = new AtomicWritable(atomic);
			aroundAAtomic.setAreaPosition(AreaPosition.AROUND_AROUND);
	//		final Direction dict = random();
			context.write(new IntWritable(HELPER.areaNumber(area)), atomic);
			
			final Position top = HELPER.getArea(new Position(HELPER.adjust(x - 1), y));
			if (!top.equals(area))
			{
				context.write(new IntWritable(HELPER.areaNumber(top)), aroundAtomic);
				
				final Position topLeft = HELPER.getArea(new Position(HELPER.adjust(x - 1), HELPER.adjust(y - 1)));
				if (!topLeft.equals(top) && !topLeft.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(topLeft)), aroundAAtomic);
				}
				
				final Position topRight = HELPER.getArea(new Position(HELPER.adjust(x - 1), HELPER.adjust(y + 1)));
				if (!topRight.equals(top) && !topRight.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(topRight)), aroundAAtomic);
				}
			}
			else
			{
				final Position topTop = HELPER.getArea(new Position(HELPER.adjust(x - 2), y));
				if (!topTop.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(topTop)), aroundAAtomic);
				}
			}
			
			final Position bottom = HELPER.getArea(new Position(HELPER.adjust(x + 1), y));
			if (!bottom.equals(area))
			{
				context.write(new IntWritable(HELPER.areaNumber(bottom)), aroundAtomic);
				
				final Position bottomLeft = HELPER.getArea(new Position(HELPER.adjust(x + 1), HELPER.adjust(y - 1)));
				if (!bottomLeft.equals(bottom) && !bottomLeft.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(bottomLeft)), aroundAAtomic);
				}
				
				final Position bottomRight = HELPER.getArea(new Position(HELPER.adjust(x + 1), HELPER.adjust(y + 1)));
				if (!bottomRight.equals(bottom) && !bottomRight.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(bottomRight)), aroundAAtomic);
				}
			}
			else
			{
				final Position bottomBottom = HELPER.getArea(new Position(HELPER.adjust(x + 2), y));
				if (!bottomBottom.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(bottomBottom)), aroundAAtomic);
				}
			}
			
			final Position left = HELPER.getArea(new Position(x, HELPER.adjust(y - 1)));
			if (!left.equals(area))
			{
				context.write(new IntWritable(HELPER.areaNumber(left)), aroundAtomic);
			}
			else
			{
				final Position leftLeft = HELPER.getArea(new Position(x, HELPER.adjust(y - 2)));
				if (!leftLeft.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(leftLeft)), aroundAAtomic);
				}
			}
			
			final Position right = HELPER.getArea(new Position(x, HELPER.adjust(y + 1)));
			if (!right.equals(area))
			{
				context.write(new IntWritable(HELPER.areaNumber(right)), aroundAtomic);
			}
			else
			{
				final Position rightRight = HELPER.getArea(new Position(x, HELPER.adjust(y + 2)));
				if (!rightRight.equals(area))
				{
					context.write(new IntWritable(HELPER.areaNumber(rightRight)), aroundAAtomic);
				}
			}
		}
	}
}
