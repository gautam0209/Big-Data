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

public class MatrixMulD {

	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
			String[] tokens = value.toString().split(",");
			String  colNum , matrixID = tokens[0],rowNum ;
			String output ;
			
			if(tokens[0].equals("D"))
			{
				colNum = tokens[1];
				output = matrixID + ","  + tokens[2];
				context.write(new Text(colNum), new Text(output));
			}
			else
			{
				rowNum = tokens[1];
				output = matrixID  + "," + tokens[3];
				context.write(new Text(rowNum), new Text(output));
			}
		}
	}
	
	public static class MatrixMulReducer extends Reducer<Text, Text, Text, Text>
	{

		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			float valD =0, valR=0;
			for(Text v:value)
			{
				String[] val = v.toString().split(",");
				if (val[0].equals("D"))
					valD = Float.parseFloat(val[1]);
				else
					valR = Float.parseFloat(val[1]);
			}
			if(valD != 0f)
			 context.write(null, new Text(Float.toString(valD*valR)));
		}
	}
	
	public static void main(String ar[])throws Exception
	{
		String input = ar[1] + "D", output = ar[3] + "D" + ar[5];
		int i = Integer.parseInt(ar[5]) -1;
			
			Configuration conf = new Configuration();
			
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(MatrixMulD.class);

			job.setMapperClass(MatrixMulMapper.class);
			job.setReducerClass(MatrixMulReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path(ar[2]+Integer.toString(i)), TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

	}
	
	}
	

