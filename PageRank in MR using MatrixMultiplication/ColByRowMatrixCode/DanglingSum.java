package cs6240;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DanglingSum {
	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
			context.write(new Text("same"), value);
		}
	}
	
	
	public static class MatrixMulReducer extends Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			Iterator<Text> itr = value.iterator();
			
			double d = 0;
			
			while(itr.hasNext())
			{
				d += Double.parseDouble(itr.next().toString());
			}
			
			context.write(null, new Text(Double.toString(d)));
		}
	}
	
	
	public static void main(String ar[])throws Exception
	{
		String input = ar[3] + "D" + ar[5], output = ar[6] + ar[5];
			
			Configuration conf = new Configuration();
			
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(DanglingSum.class);

			job.setMapperClass(MatrixMulMapper.class);

			job.setReducerClass(MatrixMulReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(input));

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

	}

}
