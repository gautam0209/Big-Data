package pageRank;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top100 {

	// Mapper
public static class Top100Mapper extends Mapper<Object, Text, NullWritable, Text> {
		TreeMap<Double, String> treeMap = new TreeMap<>();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] token = value.toString().split("\t");
			treeMap.put(Double.parseDouble(token[1]), token[0]);
			if (treeMap.size() > 100) // if size is greater than 100, delete
				// the record
				treeMap.remove(treeMap.firstKey());
		}
		
		protected void cleanup(Context context)throws IOException, InterruptedException
		{
			Iterator<Double> itr = treeMap.descendingKeySet().iterator();
			while (itr.hasNext()) {
				double key = itr.next();
				String pageName = treeMap.get(key);
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
	
	// Finally emitting top 100 records
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
public static void main(String[] folder) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Top 100");
		job.setJarByClass(Top100.class);

		job.setMapperClass(Top100Mapper.class);
		job.setReducerClass(Top100Reducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path path = new Path(folder[0]);

		FileInputFormat.addInputPath(job, path);

		FileOutputFormat.setOutputPath(job, new Path(folder[1]));

		job.waitForCompletion(true);

	}
}