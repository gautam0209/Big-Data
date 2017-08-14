package weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This class demonstrates the behavior for Secondary Sort Design in MapReduce */

public class SecondarySort {
	
	/* Class to store minTemp and maxTemp value */
	public static class MinMaxTemp implements Writable{

		private Double minTemp;
		private Double maxTemp;

		public void readFields(DataInput in) throws IOException {
			minTemp = in.readDouble();
			maxTemp = in.readDouble();
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(minTemp);
			out.writeDouble(maxTemp);
		}

		public String toString() {
			return ", " + minTemp + ", " + maxTemp;
		}

		// getter and setter methods

		public void setMinTemp(double temp) {
			minTemp = temp;
		}

		public void setMaxTemp(double temp) {
			maxTemp = temp;
		}

		public double getMinTemp() {
			return minTemp;
		}

		public double getMaxTemp() {
			return maxTemp;
		}

	}
	
	// Class for Composite Key - (stationId, year)
	
	public static class CompositeKey implements Writable,WritableComparable<CompositeKey>
	{
		private String stationId;
		private String year;
		
		// parameterized constructor
		public CompositeKey(String stationId,String year)
		{
			this.stationId = stationId;
			this.year = year;
		}
		public CompositeKey(){}
		
		public void readFields(DataInput in)throws IOException
		{
			stationId = WritableUtils.readString(in);
			year = WritableUtils.readString(in);
		}
		
		public void write(DataOutput out)throws IOException
		{
			WritableUtils.writeString(out, stationId);
			WritableUtils.writeString(out, year);
		}
		
		
		// implementing compareTo method to compare composite key
		
		public int compareTo(CompositeKey key)
		{
			int result = stationId.compareTo(key.stationId);
			if(result == 0)
				result = year.compareTo(key.year);
			return result;
		}
		
		
		// getter and setter method
		
		public String getStationId()
		{
			return stationId;
		}
		
		public String getYear()
		{
			return year;
		}
		
		public void setStationId(String stationId)
		{
			this.stationId = stationId;
		}
		
		public void setYear(String year)
		{
			this.year = year;
		}
		
		public String toString()
		{
			return stationId + "," + year;
		}
		
	}// End of MinMaxTemp class

	// Mapper Class - emitting as emit(CompositeKey, MinMaxTemp)
				
	public static class TempMapper extends Mapper<Object, Text, CompositeKey, MinMaxTemp> {

		private MinMaxTemp mmt = new MinMaxTemp();
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String record = new String(value.toString());
			String token[] = record.split(",");	// Splitting the record into array of strings
			String year = token[1].substring(0, 4); // extracting year

			// checking TMAX and TMIN values
			if (token[2].equals("TMAX")) {
				word.set(token[0]);
				mmt.setMaxTemp(Double.parseDouble(token[3]));
				mmt.setMinTemp(-9999);
				context.write(new CompositeKey(token[0],year), mmt); // emit CompositeKey, MinMaxTemp
			} else if (token[2].equals("TMIN")) {
				word.set(token[0]);
				mmt.setMinTemp(Double.parseDouble(token[3]));
				mmt.setMaxTemp(-9999);
				context.write(new CompositeKey(token[0],year), mmt); // emit CompositeKey, MinMaxTemp
			}
		}
	}
	
	//Custom Partitioner - partitioning only on the basis of stationId
	public static class CustomPartitioner extends Partitioner<CompositeKey, MinMaxTemp>
	{
		public int getPartition(CompositeKey key, MinMaxTemp val, int numOfRedTasks)
		{
			return Math.abs(key.getStationId().hashCode()%numOfRedTasks);
		}
	}
	
	// Key Sort and Comparator - Sorting on the basis of first station Id then Year
	public static class CustomKeyComparator extends WritableComparator
	{
		protected CustomKeyComparator()
		{
			super(CompositeKey.class,true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			CompositeKey key1 = (CompositeKey)w1;
			CompositeKey key2 = (CompositeKey)w2;
			int result =key1.getStationId().compareTo(key2.getStationId());
			if( result == 0)
			{
				result = key1.getYear().compareTo(key2.getYear());
			}
			return result;	
		}
	}
	
	
	// GroupingComparator groups the records on the basis of station Id 
	
	public static class CustomGroupingComparator extends WritableComparator
	{
		public CustomGroupingComparator()
		{
			super(CompositeKey.class,true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			CompositeKey key1 = (CompositeKey)w1;
			CompositeKey key2 = (CompositeKey)w2;
			return key1.getStationId().compareTo(key2.getStationId());
		}
		
	}

	// Reducer Class - Input (CompositeKey, MinMaxTemp), Output (String,String)

	public static class TempReducer extends Reducer<CompositeKey, MinMaxTemp, String, String> {

		@Override
		public void reduce(CompositeKey key, Iterable<MinMaxTemp> values, Context context)
				throws IOException, InterruptedException {
			
			double minSum = 0, maxSum = 0;
			int minCount = 0, maxCount = 0;
			String year = key.getYear();
			StringBuilder sb = new StringBuilder(", ["); // For formatting and storing values

			for (MinMaxTemp val : values) {
				
				// checking if year is changing in composite key
				if(! (key.getYear().equals(year)))
				{
					// appending formatted output value for particular year in StringBuilder
					sb.append("(" + year + ", " + (minSum/minCount) + ", " + (maxSum/maxCount) + "), ");
					
					year = key.getYear(); //updating year
					// initializing variables for changed year
					minCount = 0;
					maxCount = 0;
					minSum = 0;
					maxSum =0;
				}
					
				// Incrementing max Count and adding sum for Maximum temperature records
				if (val.getMinTemp() == -9999) {
					maxCount++;
					maxSum += val.getMaxTemp();
				}
				
				// Incrementing min Count and adding sum for Minimum temperature records
				if (val.getMaxTemp() == -9999) {
					minCount++;
					minSum += val.getMinTemp();
				}

			}

			// Updating string builder for last year record for particular station ID
			sb.append("(" + year + ", " + (minSum/minCount) + ", " + (maxSum/maxCount) + ")]");
			
			//emit stationId and String with formatted output year wise
			context.write(key.getStationId(), sb.toString());
		}
	}

	// Driver method
	
		public static void main(String ar[])throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Secondary Sort");
		job.setJarByClass(SecondarySort.class);
		
		job.setMapperClass(TempMapper.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setGroupingComparatorClass(CustomGroupingComparator.class);
		job.setSortComparatorClass(CustomKeyComparator.class);
		job.setReducerClass(TempReducer.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(MinMaxTemp.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(ar[0]));
		FileOutputFormat.setOutputPath(job, new Path(ar[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
