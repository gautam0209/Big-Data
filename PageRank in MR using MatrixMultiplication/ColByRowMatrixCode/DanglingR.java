package cs6240;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DanglingR {
	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		static long count1;
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			count1 = Long.parseLong(conf.get("nodes")); 
		}
		
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
				String[] tokens = value.toString().split(",");
				if(tokens.length == 1)
				{
					for(int i =0; i<count1;i++)
						context.write(new Text(Integer.toString(i) + "," + "0"), new Text(tokens[0]));
				}
				else
					context.write(new Text(tokens[1] + "," + tokens[2]), new Text(tokens[3]));
		}
	}
	
	
	public static class MatrixMulReducer extends Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			float sum =0;
			
			for(Text v : value)
			{
				sum += Float.parseFloat(v.toString());
			}
			String[] keys = key.toString().split(",");
			String outputV = "R" + "," + keys[0] + "," + keys[1] + "," + Float.toString(sum);
			context.write(null, new Text(outputV));
		}
	}
	
	
	public static void main(String ar[], long nodes)throws Exception
	{
		String input = ar[2] + ar[5] + "D", output = ar[2] + ar[5];
			
			Configuration conf = new Configuration();
			
			conf.set("nodes", Long.toString(nodes) );
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(DanglingR.class);

			job.setMapperClass(MatrixMulMapper.class);
			job.setReducerClass(MatrixMulReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path(ar[6] + ar[5]), TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

	}

}
