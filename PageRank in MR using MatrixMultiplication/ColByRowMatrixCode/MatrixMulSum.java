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


public class MatrixMulSum {
	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
				String[] tokens = value.toString().split("\t");
				context.write(new Text(tokens[0]), new Text(tokens[1]));
		}
	}
	
	
	public static class MatrixMulReducer extends Reducer<Text, Text, Text, Text>
	{
		static long count1;
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			count1 = Long.parseLong(conf.get("nodes")); 
		}
		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			double alpha = 0.15;
			double count =count1;
			Iterator<Text> itr = value.iterator();
			String[] token = key.toString().split(",");
			String rowNum = token[0], colNum = token[1];
			double d = 0;
			while(itr.hasNext())
			{
				d += Double.parseDouble(itr.next().toString());
			}
			d = alpha/count + (1-alpha)*d;
			
			context.write(null, new Text("R" + "," + rowNum + "," + colNum + "," + d  ));
		}
	}
	
	
	public static void main(String ar[], long nodes)throws Exception
	{
		String input = ar[3] + ar[5], output = ar[2] + ar[5] + "D";
			
			Configuration conf = new Configuration();
			
			conf.set("nodes", Long.toString(nodes) );
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(MatrixMulSum.class);

			job.setMapperClass(MatrixMulMapper.class);
			job.setReducerClass(MatrixMulReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path path = new Path(input);

			FileInputFormat.addInputPath(job, path);

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

	}

}
