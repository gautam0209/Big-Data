package cs6240;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMul {

	
	public static class MatrixMulMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context)throws InterruptedException, IOException
		{
			String[] tokens = value.toString().split(",");
			String rowNum = tokens[1], colNum = tokens[2], matrixID = tokens[0];
			String output ;
			
				output = matrixID + "," + colNum + ","  + tokens[3];
				context.write(new Text(rowNum), new Text(output));
		}
	}
	
	public static class MatrixMulReducer extends Reducer<Text, Text, Text, Text>
	{
		Map<Integer, Float> kvMap = new HashMap<Integer,Float>();
		static long count;
		static int itr;
		public void setup(Context context)throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			count = Long.parseLong(conf.get("nodes"));
			itr = Integer.parseInt(conf.get("itr"));
			if(itr == 1)
			{
				float val = 1f/count;
				for(int i =0 ; i<count; i++)
					kvMap.put(i, val);
			}
			else{
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
						String[] keyVal = line.split(",");
						Integer key = Integer.parseInt(keyVal[0]);
						Float val = Float.parseFloat(keyVal[1]);
						kvMap.put(key, val);
						
					}
				}
				catch(Exception e){}
			}
			}
			
		}

		
		public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
		{
			List<Pair<Integer,Float>> listM = new ArrayList<Pair<Integer,Float>>();
			
			float alpha =0.15f;
			
			for(Text v : value)
			{
				String[] values = v.toString().split(",");
					listM.add(new Pair<Integer,Float>(Integer.parseInt(values[1]), Float.parseFloat(values[2])));

			}
			
			Iterator<Pair<Integer,Float>> itrM = listM.iterator();

			
			Pair<Integer,Float> valM = new Pair<Integer,Float>(0,0f);
			String outKey;

			
			
			float sum =0;
			outKey = key.toString() + "," + "0";
			while(itrM.hasNext())
			{
				float pairRV =0;
				valM = itrM.next();
				int pairMK = valM.getKey();
				float pairMV = valM.getValue();
				 pairRV = kvMap.get(pairMK);
				
				sum += pairMV*pairRV;
			}
			sum = alpha/count + (1-alpha)*sum;
			context.write(null, new Text(outKey + "," + Float.toString(sum)));
			
		}
	}
	
	public static void main(String ar[], long nodes, int itr)throws Exception
	{
		String input = ar[1], output = ar[2] + ar[5] + "D";
		int i = Integer.parseInt(ar[5]) -1;
			
			Configuration conf = new Configuration();
			
			conf.set("nodes", Long.toString(nodes) );
			conf.set("itr", Integer.toString(itr));
			
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(MatrixMul.class);

			job.setMapperClass(MatrixMulMapper.class);
			job.setReducerClass(MatrixMulReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			if(itr != 1){
				Path file = new Path(ar[2] + Integer.toString(i));
				job.addCacheFile(file.toUri());}

			job.waitForCompletion(true);

	}
	
	}
	

