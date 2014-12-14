package com.left.peter.atomic;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;
import org.junit.Before;
import org.junit.Test;

import com.left.peter.atomic.AtomicReducer;
import com.left.peter.atomic.AtomicWritable;
import com.left.peter.data.AreaPosition;
import com.left.peter.data.Constants;
import com.left.peter.data.Direction;
import com.left.peter.data.Position;


public class AtomicReducerTest
{
	private List<Text> result = new ArrayList<Text>();
	
	private class TestRecordWriter extends RecordWriter<NullWritable, Text>
	{

		@Override
		public void write(NullWritable key, Text value) throws IOException,
				InterruptedException {
			result.add(value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			
		}
	}
	
	private static class TestReducer extends AtomicReducer{
		private class Context extends Reducer.Context
		{

			public Context(Configuration conf, TaskAttemptID taskid,
					RawKeyValueIterator input, Counter inputKeyCounter,
					Counter inputValueCounter, RecordWriter output,
					OutputCommitter committer, StatusReporter reporter,
					RawComparator comparator, Class keyClass, Class valueClass)
					throws IOException, InterruptedException {
				super(conf, taskid, input, inputKeyCounter, inputValueCounter, output,
						committer, reporter, comparator, keyClass, valueClass);
				// TODO Auto-generated constructor stub
			}
		}
		
		private Context context;
		TestReducer(final RecordWriter<NullWritable, Text> writer) throws IOException, InterruptedException
		{
			
				context = new TestReducer.Context(new Configuration(), new TaskAttemptID(), new RawKeyValueIterator(){

					@Override
					public DataInputBuffer getKey() throws IOException {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public DataInputBuffer getValue() throws IOException {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public boolean next() throws IOException {
						// TODO Auto-generated method stub
						return false;
					}

					@Override
					public void close() throws IOException {
						// TODO Auto-generated method stub
						
					}

					@Override
					public Progress getProgress() {
						// TODO Auto-generated method stub
						return null;
					}
					
				}, null, null,
						writer, null, null, null, NullWritable.class, Text.class);
		}
	}
	
	private TestReducer reducer;
	
	@Before
	public void setUp() throws IOException, InterruptedException
	{
		reducer = new TestReducer(new TestRecordWriter());
		Constants.NEW_ATOMICS.setValue(0);
		result.clear();
	}
	
	private List<Direction> createDirections(final Direction ... dicts)
	{
		final List<Direction> results = new ArrayList<>();
		
		for (final Direction item : dicts)
		{
			results.add(item);
		}
		
		return results;
	}
	
	@Test
	public void oneOutsideAtomic_itShould_moveInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.LEFT), AreaPosition.AROUND, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("0,9"));
	}
	
	@Test
	public void oneOutsideAtomic_itShould_keepoutside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.RIGHT), AreaPosition.AROUND, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(0));
	}
	
	@Test
	public void oneBoundaryAtomic_itShould_KeepInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 9), createDirections(Direction.DOWN), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("1,9"));
	}
	
	@Test
	public void oneBoundaryAtomic_itShould_GoOutside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 9), createDirections(Direction.UP), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(0));
	}
	
	@Test
	public void oneInsideAtomic_itShould_keepInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.LEFT), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("4,3"));
	}
	
	@Test
	public void bond1InsideAtomic_bothShould_Stay() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.LEFT), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.LEFT), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(2));
		assertThat(result.get(0).toString(), equalTo("4,4"));
		assertThat(result.get(1).toString(), equalTo("4,5"));
	}
	
	@Test
	public void bond1InsideAtomic_oneShouldMove_oneShouldStay() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.LEFT), AreaPosition.INSIDE, true, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(2));
		assertThat(result.get(0).toString(), equalTo("4,3"));
		assertThat(result.get(1).toString(), equalTo("4,5"));
	}
	
	@Test
	public void bond1InsideAtomic_bothShould_move() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.LEFT), AreaPosition.INSIDE, true, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.RIGHT), AreaPosition.INSIDE, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(2));
		assertThat(result.get(0).toString(), equalTo("4,3"));
		assertThat(result.get(1).toString(), equalTo("4,6"));
	}
	
	@Test
	public void bond1InsideAtomic_oneShould_choose2ndDirection() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.RIGHT, Direction.LEFT), AreaPosition.INSIDE, true, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.RIGHT), AreaPosition.INSIDE, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(2));
		assertThat(result.get(0).toString(), equalTo("4,3"));
		assertThat(result.get(1).toString(), equalTo("4,6"));
	}
	
	@Test
	public void bond1OutsideAtomic_itShould_moveInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.LEFT, Direction.RIGHT), AreaPosition.AROUND, true, false, false));
		input.add(new AtomicWritable(new Position(0, 11), createDirections(Direction.RIGHT), AreaPosition.AROUND_AROUND, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("0,9"));
	}
	
	@Test
	public void bond1OutsideAtomic_itShould_keepOutside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.LEFT, Direction.RIGHT), AreaPosition.AROUND, false, false, false));
		input.add(new AtomicWritable(new Position(0, 11), createDirections(Direction.RIGHT), AreaPosition.AROUND_AROUND, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(0));
	}
	
	@Test
	public void bond1BoundaryAtomic_itShould_moveOutside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 9), createDirections(Direction.LEFT, Direction.RIGHT), AreaPosition.INSIDE, true, false, false));
		input.add(new AtomicWritable(new Position(0, 8), createDirections(Direction.RIGHT), AreaPosition.INSIDE, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("0,8"));
	}
	
	@Test
	public void bond1BoundaryAtomic_itShould_stayInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 9), createDirections(Direction.RIGHT, Direction.LEFT), AreaPosition.INSIDE, true, false, false));
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.RIGHT), AreaPosition.INSIDE, true, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result.get(0).toString(), equalTo("0,8"));
	}
	
	@Test
	public void bond3InsideAtomic_itShould_stay() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.RIGHT, Direction.LEFT, Direction.DOWN, Direction.UP), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(3, 4), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(4, 3), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(4));
		assertThat(result, hasItems(new Text("4,4"), new Text("3,4"), new Text("4,3"), new Text("4,5")));
	}
	
	@Test
	public void bond3InsideAtomic_itShould_move() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(4, 4), createDirections(Direction.RIGHT, Direction.LEFT, Direction.DOWN, Direction.UP), AreaPosition.INSIDE, false, false, true));
		input.add(new AtomicWritable(new Position(3, 4), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(4, 3), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		input.add(new AtomicWritable(new Position(4, 5), createDirections(Direction.RIGHT), AreaPosition.INSIDE, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(4));
		assertThat(result, hasItems(new Text("5,4"), new Text("3,4"), new Text("4,3"), new Text("4,5")));
	}
	
	@Test
	public void bond3AroundAtomic_itShould_moveInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(1, 10), createDirections(Direction.RIGHT, Direction.LEFT, Direction.DOWN, Direction.UP), AreaPosition.AROUND, false, false, true));
		input.add(new AtomicWritable(new Position(1, 11), createDirections(Direction.RIGHT), AreaPosition.AROUND_AROUND, false, false, false));
		input.add(new AtomicWritable(new Position(2, 10), createDirections(Direction.RIGHT), AreaPosition.AROUND, false, false, false));
		input.add(new AtomicWritable(new Position(0, 10), createDirections(Direction.RIGHT), AreaPosition.AROUND, false, false, false));
		
		reducer.reduce(new IntWritable(0), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result, hasItems(new Text("1,9")));
	}
	
	@Test
	public void error_itShould_moveInside() throws IOException, InterruptedException
	{
		final List<AtomicWritable> input = new ArrayList<AtomicWritable>();
		input.add(new AtomicWritable(new Position(0, 0), createDirections(Direction.LEFT, Direction.RIGHT, Direction.DOWN, Direction.UP), AreaPosition.AROUND, false, false, true));
		
		reducer.reduce(new IntWritable(Constants.AREA_NUM.getValue() - 1), input, reducer.context);
		assertThat(result.size(), equalTo(1));
		assertThat(result, hasItems(new Text(String.format("%d,%d", 0, Constants.AREA_LEN.getValue() * Constants.AREA_NUM.getValue() - 1))));
	}
}
