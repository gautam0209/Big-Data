package cs6240;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Objects;

public class Top100 {
	

	// Mapper
public static class Top100Mapper extends Mapper<Object, Text, NullWritable, Text> {
		TreeMap<Double, Integer> treeMap = new TreeMap<>();
		
		Map<String, Integer> kvMap = new HashMap<String,Integer>();
		
		public void setup(Context context)throws IOException, InterruptedException
		{	
			
			Configuration conf = context.getConfiguration();
			//Path[] distCache = DistributedCache.getLocalCacheFiles(conf);
			URI[] uri = context.getCacheFiles();
			
			FileSystem fs =  FileSystem.get(uri[0],conf );

			FileStatus[] fstatus = fs.listStatus(new Path(uri[0])); 
			for(int i=0; i<fstatus.length; i++)
			{
				Path p = fstatus[i].getPath();
				//System.out.println("Path is:" + p.toString());
				String line = new String();
				try{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
					//System.out.println("HEllo");
					while( (line = br.readLine())!=null)
					{
						String[] keyVal = line.split("\t");
						kvMap.put(keyVal[0],Integer.parseInt(keyVal[1]));
					}
				}
				catch(Exception e){}
			}
		}

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] token = value.toString().split(",");

			treeMap.put(Double.parseDouble(token[3]), Integer.parseInt(token[1]));
			
			if (treeMap.size() > 100) // if size is greater than 100, delete
				// the record
				treeMap.remove(treeMap.firstKey());
		}
		
		protected void cleanup(Context context)throws IOException, InterruptedException
		{
			Iterator<Double> itr = treeMap.descendingKeySet().iterator();
			while (itr.hasNext()) {
				double key = itr.next();
				int index = treeMap.get(key);
				String pageName = "";
				for(Entry<String,Integer> e : kvMap.entrySet())
					if(Objects.equal(index, e.getValue()))
						pageName = e.getKey();
				context.write(NullWritable.get(), new Text(pageName + "|" + key) );
		}
			
	}
}

// Reducer
public static class Top100Reducer extends Reducer<NullWritable, Text, Text, Text> {
	TreeMap<Double, String> treeMap = new TreeMap<>();
	
	
	public void reduce(NullWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		for(Text val : value)
		{
		String[] token = val.toString().split("\\|");
		
		treeMap.put(Double.parseDouble(token[1]), token[0]);
		if (treeMap.size() > 100) // if size is greater than 100, delete
			// the record
			treeMap.remove(treeMap.firstKey());
	}
	}
	
	protected void cleanup(Context context)throws IOException, InterruptedException
	{
		Iterator<Double> itr = treeMap.descendingKeySet().iterator();
		while (itr.hasNext()) {
			double key = itr.next();
			String pageName = treeMap.get(key);
			context.write(new Text(pageName), new Text(Double.toString(key)) );
	}
		
}
}


// overloaded main function
public static void main(String[] ar) throws Exception {
		
		Configuration conf = new Configuration();

		String output = new String();
		
		Job job = Job.getInstance(conf, "Top 100");
		job.setJarByClass(Top100.class);

		job.setMapperClass(Top100Mapper.class);
		job.setReducerClass(Top100Reducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path file = new Path(ar[4]);
 		

		Path path = new Path(ar[2] + ar[5]);

		output = ar[2] + "100";
		FileInputFormat.addInputPath(job, path);

		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.addCacheFile(file.toUri());
		
		job.waitForCompletion(true);

	}
}