package cs6240;

import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.io.InputStreamReader;

import java.util.HashMap;

import java.util.Map;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MatrixCreatorD {
	
	
	public static class MatrixMapper extends Mapper<Object, Text, Text, Text>
	{
		Map<String, Integer> kvMap = new HashMap<String,Integer>();
		
		public void setup(Context context)throws IOException, InterruptedException
		{	
			
			Configuration conf = context.getConfiguration();
			URI[] uri = context.getCacheFiles();
			
			FileSystem fs =  FileSystem.get(uri[0],conf );

			FileStatus[] fstatus = fs.listStatus(new Path(uri[0])); 
			for(int i=0; i<fstatus.length; i++)
			{
				Path p = fstatus[i].getPath();

				String line = new String();
				try{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));

					while( (line = br.readLine())!=null)
					{
						String[] keyVal = line.split("\t");
						kvMap.put(keyVal[0],Integer.parseInt(keyVal[1]));
					}
				}
				catch(Exception e){}
			}
		}

		public void map(Object key, Text value, Context context)throws IOException, InterruptedException
		{ 
			String[] tokens = value.toString().split("\t");
			int index =0;
			index = kvMap.get(tokens[0]);
			if(tokens.length == 1)
			{	
			context.write(new Text(tokens[0]),new Text(Integer.toString(index))) ;
			}
		}
	}
	
	public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
	{
		static long count;
		
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			count = Long.parseLong(conf.get("nodes")); 
		}

		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{ 
			int outKey =0;
			for(Text v : value)
			{
				 outKey = Integer.parseInt(v.toString());
				
			}
			String outVal = "D" + "," + Integer.toString(outKey) + "," + Float.toString(1f/count);
			context.write(null, new Text(outVal));
		}
	}
	
	
	public static void main(String ar[],long nodes)throws Exception
	{
		String input = ar[0], output = ar[1] + "D";
			
			Configuration conf = new Configuration();
			Path file = new Path(ar[4]);
			
			conf.set("nodes", Long.toString(nodes) );
			
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(MatrixCreatorD.class);

			job.setMapperClass(MatrixMapper.class);
			job.setReducerClass(MatrixReducer.class);
			
			

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			

			Path path = new Path(input);

			FileInputFormat.addInputPath(job, path);

			FileOutputFormat.setOutputPath(job, new Path(output));
			
			job.addCacheFile(file.toUri());
			
			job.waitForCompletion(true);

	}

}
