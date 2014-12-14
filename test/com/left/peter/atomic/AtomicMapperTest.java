package com.left.peter.atomic;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Before;
import org.junit.Test;

import com.left.peter.atomic.AtomicMapper;
import com.left.peter.atomic.AtomicWritable;
import com.left.peter.data.AreaPosition;
import com.left.peter.data.Constants;
import com.left.peter.data.Position;

public class AtomicMapperTest
{
	private Map<IntWritable, AtomicWritable> map = new HashMap<IntWritable, AtomicWritable>();
	
	private class TestRecordWriter extends RecordWriter<IntWritable, AtomicWritable>
	{

		@Override
		public void write(IntWritable key, AtomicWritable value) throws IOException,
				InterruptedException {
			map.put(key, value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			
		}
	}
	
	private static class TestMapper extends AtomicMapper{
		private class Context extends Mapper.Context
		{

			public Context(Configuration conf, TaskAttemptID taskid,
					RecordReader<LongWritable, Text> reader, RecordWriter<IntWritable, AtomicWritable> writer,
					OutputCommitter committer, StatusReporter reporter,
					InputSplit split) throws IOException, InterruptedException {
				super(conf, taskid, reader, writer, committer, reporter, split);
				
				conf = new Configuration();
			}
		}
		
		private Context context;
		TestMapper(final RecordWriter<IntWritable, AtomicWritable> writer) throws IOException, InterruptedException
		{
			
				context = new TestMapper.Context(new Configuration(), new TaskAttemptID(), null,
						writer, null, null, null);
		}
	}
	
	private TestMapper mapper;
	
	@Before
	public void setUp() throws IOException, InterruptedException
	{
		mapper = new TestMapper(new TestRecordWriter());
		map.clear();
	}
	
	private void checkResult(final int key, final int x, final int y, final AreaPosition area)
	{
		assertThat(map.containsKey(new IntWritable(key)), equalTo(true));
		final AtomicWritable atomic = map.get(new IntWritable(key));
		assertThat(atomic.getPos(), equalTo(new Position(x, y)));
		assertThat(atomic.getAreaPosition(), equalTo(area));
	}	
	
	@Test
	public void leftAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,8"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 7, 8, AreaPosition.INSIDE);
		checkResult(1, 7, 8, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void rightAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,11"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(1, 7, 11, AreaPosition.INSIDE);
		checkResult(0, 7, 11, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("8,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 8, 2, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 8, 2, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("11,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 11, 2, AreaPosition.AROUND_AROUND);
		checkResult(Constants.AREA_NUM.getValue(), 11, 2, AreaPosition.INSIDE);
	}
	
	
	@Test
	public void leftCircleAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		final int y = Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue() - 2;
		mapper.map(new LongWritable(0), 
				new Text(String.format("7,%d", y)), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(Constants.AREA_NUM.getValue() - 1, 7, y, AreaPosition.INSIDE);
		checkResult(0, 7, y, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void rightCircleAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,1"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 7, 1, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() - 1, 7, 1, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topCircleAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		final int x = Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue() - 2;
		mapper.map(new LongWritable(0), 
				new Text(String.format("%d,2", x)), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), x, 2, AreaPosition.INSIDE);
		checkResult(0, x, 2, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomCircleAroundAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("1,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 1, 2, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 1, 2, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void leftAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,9"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 7, 9, AreaPosition.INSIDE);
		checkResult(1, 7, 9, AreaPosition.AROUND);
	}
	
	@Test
	public void rightAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,10"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(1, 7, 10, AreaPosition.INSIDE);
		checkResult(0, 7, 10, AreaPosition.AROUND);
	}
	
	@Test
	public void topAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("9,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 9, 2, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 9, 2, AreaPosition.AROUND);
	}
	
	@Test
	public void bottomAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("10,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 10, 2, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue(), 10, 2, AreaPosition.INSIDE);
	}
	
	
	@Test
	public void leftCircleAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		final int y = Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue() - 1;
		mapper.map(new LongWritable(0), 
				new Text(String.format("7,%d", y)), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(Constants.AREA_NUM.getValue() - 1, 7, y, AreaPosition.INSIDE);
		checkResult(0, 7, y, AreaPosition.AROUND);
	}
	
	@Test
	public void rightCircleAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("7,0"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 7, 0, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() - 1, 7, 0, AreaPosition.AROUND);
	}
	
	@Test
	public void topCircleAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		final int x = Constants.AREA_NUM.getValue() * Constants.AREA_LEN.getValue() - 1;
		mapper.map(new LongWritable(0), 
				new Text(String.format("%d,2", x)), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), x, 2, AreaPosition.INSIDE);
		checkResult(0, x, 2, AreaPosition.AROUND);
	}
	
	@Test
	public void bottomCircleAroundNode_itShould_return2Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("0,2"), mapper.context);
		assertThat(map.values().size(), equalTo(2));
		checkResult(0, 0, 2, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 0, 2, AreaPosition.AROUND);
	}
	
	@Test
	public void topLeftCornerNode_itShould_return4Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("0,0"), mapper.context);
		assertThat(map.values().size(), equalTo(4));
		
		checkResult(0, 0, 0, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 0, 0, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 0, 0, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() * Constants.AREA_NUM.getValue() - 1, 0, 0, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topRightCornerNode_itShould_return4Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("0,9"), mapper.context);
		assertThat(map.values().size(), equalTo(4));
		
		checkResult(0, 0, 9, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 0, 9, AreaPosition.AROUND);
		checkResult(1, 0, 9, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1) + 1, 0, 9, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomLeftCornerNode_itShould_return4Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("9,0"), mapper.context);
		assertThat(map.values().size(), equalTo(4));
		
		checkResult(0, 9, 0, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 9, 0, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 9, 0, AreaPosition.AROUND);
		checkResult(2 * Constants.AREA_NUM.getValue() - 1, 9, 0, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomRightCornerNode_itShould_return4Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("9,9"), mapper.context);
		assertThat(map.values().size(), equalTo(4));
		
		checkResult(0, 9, 9, AreaPosition.INSIDE);
		checkResult(1, 9, 9, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue(), 9, 9, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() + 1, 9, 9, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topLeft1Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("0,1"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 0, 1, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 0, 1, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 0, 1, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topLeft2Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("1,0"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 1, 0, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 1, 0, AreaPosition.AROUND_AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 1, 0, AreaPosition.AROUND);
	}
	
	@Test
	public void topRight1Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("0,8"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 0, 8, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 0, 8, AreaPosition.AROUND);
		checkResult(1, 0, 8, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void topRight2Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("1,9"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 1, 9, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue() * (Constants.AREA_NUM.getValue() - 1), 1, 9, AreaPosition.AROUND_AROUND);
		checkResult(1, 1, 9, AreaPosition.AROUND);
	}
	
	@Test
	public void bottomLeft1Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("9,1"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 9, 1, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 9, 1, AreaPosition.AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 9, 1, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomLeft2Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("8,0"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 8, 0, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 8, 0, AreaPosition.AROUND_AROUND);
		checkResult(Constants.AREA_NUM.getValue() - 1, 8, 0, AreaPosition.AROUND);
	}
	
	@Test
	public void bottomRight1Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("9,8"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 9, 8, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 9, 8, AreaPosition.AROUND);
		checkResult(1, 9, 8, AreaPosition.AROUND_AROUND);
	}
	
	@Test
	public void bottomRight2Node_itShould_return3Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), 
				new Text("8,9"), mapper.context);
		assertThat(map.values().size(), equalTo(3));
		
		checkResult(0, 8, 9, AreaPosition.INSIDE);
		checkResult(Constants.AREA_NUM.getValue(), 8, 9, AreaPosition.AROUND_AROUND);
		checkResult(1, 8, 9, AreaPosition.AROUND);
	}
	
	@Test
	public void insideNode_itShould_return1Area() throws IOException, InterruptedException
	{
		mapper.map(new LongWritable(0), new Text("2,2"), mapper.context);
		assertThat(map.values().size(), equalTo(1));
		checkResult(0, 2, 2, AreaPosition.INSIDE);
	}
}
