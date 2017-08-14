package cs6240;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMul {

	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
			String[] tokens = value.toString().split(",");
			String rowNum = tokens[1], colNum = tokens[2], matrixID = tokens[0];
			String output ;
			
			if(tokens[0].equals("M"))
			{
				output = matrixID + "," + rowNum + ","  + tokens[3];
				context.write(new Text(colNum), new Text(output));
			}
			else
			{
				output = matrixID + "," + colNum + "," + tokens[3];
				context.write(new Text(rowNum), new Text(output));
			}
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
			List<Pair<Integer,Float>> listM = new ArrayList<Pair<Integer,Float>>();
			List<Pair<Integer,Float>> listR = new ArrayList<Pair<Integer,Float>>();

			boolean dang =true;
			
			for(Text v : value)
			{
				String[] values = v.toString().split(",");
				if(values[0].equals("M"))
				{
					listM.add(new Pair<Integer,Float>(Integer.parseInt(values[1]), Float.parseFloat(values[2])));
				}
				else{
					listR.add(new Pair<Integer,Float>(Integer.parseInt(values[1]), Float.parseFloat(values[2])));
				}
			}
			
			Iterator<Pair<Integer,Float>> itrM = listM.iterator();
			Iterator<Pair<Integer,Float>> itrR = listR.iterator();

			
			Pair<Integer,Float> valM = new Pair<Integer,Float>(0,0f);
			Pair<Integer,Float> valR = new Pair<Integer,Float>(0,0f);
			String outKey, outVal;
			
			while(itrM.hasNext())
			{
				dang =false;
				valM = itrM.next();
				int pairMK = valM.getKey();
				float pairMV = valM.getValue();
				itrR = listR.iterator();
	
				while(itrR.hasNext())
				{
					valR = itrR.next();
					int pairRK = valR.getKey();
					float pairRV = valR.getValue();
					outKey = Integer.toString(pairMK) + "," + Integer.toString(pairRK);

					outVal = Float.toString(pairMV * pairRV);

					context.write(new Text(outKey), new Text(outVal));
				}
			}

			if(dang)
			{
				context.write(new Text(key.toString() + "," + "0"), new Text("0"));
			}
			
		}
	}
	
	public static void main(String ar[], long nodes)throws Exception
	{
		String input = ar[1], output = ar[3] + ar[5];
		int i = Integer.parseInt(ar[5]) -1;
			
			Configuration conf = new Configuration();
			
			conf.set("nodes", Long.toString(nodes) );
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(MatrixMul.class);

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
	

