package cs6240;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class NodeIndex {
	
enum GC { INDEX };
	
	public static class MapMapper extends Mapper<Object, Text, Text, Text>
	{

		public void map(Object key, Text value, Context context)throws IOException, InterruptedException
		{ 
			String[] tokens = value.toString().split("\t");
			if(tokens.length>1)
			{	
			String[] edges = tokens[1].split("\\|");
			for(String e : edges)
			{
				context.write(new Text(e),new Text("dummy"));
			}
			}
			context.write(new Text(tokens[0]),new Text("dummy")) ;
		}
	}

	
	public static class MapReducer extends Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{ 
			long index;
			index = context.getCounter(GC.INDEX).getValue();
			context.write(key, new Text(Long.toString(index)));	
			context.getCounter(GC.INDEX).increment(1);
		}
	}
	
	public static void main(String ar[])throws Exception
	{
		String input = ar[0], output = ar[4];
			
			Configuration conf = new Configuration();
			
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(NodeIndex.class);

			job.setMapperClass(MapMapper.class);
			job.setReducerClass(MapReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(1);

			Path path = new Path(input);

			FileInputFormat.addInputPath(job, path);

			FileOutputFormat.setOutputPath(job, new Path(output));
			
			

			job.waitForCompletion(true);

	}

}
