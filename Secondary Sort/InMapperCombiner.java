package weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This class demonstrates the behavior for InMapper Combiner Class */

public class InMapperCombiner {

	/* Class to store minTemp and maxTemp value */
	public static class MinMaxTemp implements Writable {

		private Double minTemp;
		private Double maxTemp;
		private Integer minCount = 0, maxCount = 0;// Initialized to one, if
													// mapper o/p directly go to
													// reducer

		public void readFields(DataInput in) throws IOException {
			minTemp = in.readDouble();
			maxTemp = in.readDouble();
			minCount = in.readInt();
			maxCount = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(minTemp);
			out.writeDouble(maxTemp);
			out.writeInt(minCount);
			out.writeInt(maxCount);
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

		public void setMinCount(int count) {
			minCount = count;
		}

		public void setMaxCount(int count) {
			maxCount = count;
		}

		public int getMinCount() {
			return minCount;
		}

		public int getMaxCount() {
			return maxCount;
		}

	}// End of MinMaxTemp class

	// Mapper Class - emitting as emit(Text, MinMaxTemp)

	public static class TempMapper extends Mapper<Object, Text, Text, MinMaxTemp> {

		// HashMap Data Structure is used to store the stationId and Temp
		// records as values
		private Map<Text, MinMaxTemp> map = new HashMap<>();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			MinMaxTemp mmt;
			Text stationId = new Text();

			String record = new String(value.toString());
			String token[] = record.split(","); // splitting records on basis of
												// ','
			double maxTemp = 0, minTemp = 0;
			int minCount = 0, maxCount = 0;

			stationId.set(token[0]);
			// if stationId is already there in map take values from the map
			if (map.containsKey(stationId)) {
				mmt = map.get(stationId);
				maxTemp = mmt.getMaxTemp();
				minTemp = mmt.getMinTemp();
				minCount = mmt.getMinCount();
				maxCount = mmt.getMaxCount();
				if (minTemp == -9999) // Updating minTemp to zero for MaxTemp
										// records
					minTemp = 0;
				if (maxTemp == -9999) // Updating MaxTemp to zero for MinTemp
										// records
					maxTemp = 0;

			} else {
				mmt = new MinMaxTemp(); // create new object to store temperature
										// records
			}

			// Checking for TMax and TMin records

			if (token[2].equals("TMAX")) {
				mmt.setMaxTemp(Double.parseDouble(token[3]) + maxTemp);
				mmt.setMaxCount(maxCount + 1);
				if (minCount == 0)
					mmt.setMinTemp(-9999);
				map.put(stationId, mmt); // store stationId and MinMaxTemp
											// object in map
			} else if (token[2].equals("TMIN")) {
				mmt.setMinTemp(Double.parseDouble(token[3]) + minTemp);
				mmt.setMinCount(minCount + 1);
				if (maxCount == 0)
					mmt.setMaxTemp(-9999);
				map.put(stationId, mmt); // store stationId and MinMaxTemp
											// object in map
			}
		}

		// Cleanup method to get executed at the of map task to write records to
		// reducer
		public void cleanup(Context context) throws IOException, InterruptedException {

			// Iterate over map and return key and MinMaxTemp object
			Iterator<Text> itr = map.keySet().iterator();
			while (itr.hasNext()) {
				Text key = itr.next();
				context.write(key, map.get(key));
			}
		}
	}

	// Reducer Class - Input (Text, MinMaxTemp), Output (Text,MinMaxTemp)

	public static class TempReducer extends Reducer<Text, MinMaxTemp, Text, MinMaxTemp> {

		@Override
		public void reduce(Text key, Iterable<MinMaxTemp> values, Context context)
				throws IOException, InterruptedException {
			MinMaxTemp result = new MinMaxTemp();
			double minSum = 0, minAvg = 0, maxAvg = 0, maxSum = 0;
			int minCount = 0, maxCount = 0;

			// Tasks are similar to Combiner, only difference while counting
			// here it is taking the count from MinMaxTemp object and adding it.
			for (MinMaxTemp val : values) {
				if (val.getMinTemp() == -9999) {
					maxCount += val.getMaxCount();
					maxSum += val.getMaxTemp();
				} else if (val.getMaxTemp() == -9999) {
					minCount += val.getMinCount();
					minSum += val.getMinTemp();
				} else {// MinMaxTemp object contains both min and max value
						// through combiner
					maxCount += val.getMaxCount();
					maxSum += val.getMaxTemp();
					minCount += val.getMinCount();
					minSum += val.getMinTemp();

				}
			}

			// Calculating min average and max average per station Id
			minAvg = minSum / minCount;
			maxAvg = maxSum / maxCount;

			result.setMinTemp(minAvg);
			result.setMaxTemp(maxAvg);
			context.write(key, result); // emitting (Key -> StationId, result ->
										// minTempAvg, maxTempAvg)
		}
	}

	// Driver Function
	
	public static void main(String ar[])throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"In Mapper Combiner");
		job.setJarByClass(InMapperCombiner.class);
		
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MinMaxTemp.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxTemp.class);
		
		FileInputFormat.addInputPath(job, new Path(ar[0]));
		FileOutputFormat.setOutputPath(job, new Path(ar[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
