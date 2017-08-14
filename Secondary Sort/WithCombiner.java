package weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* Class using Combiner Function */

public class WithCombiner {

	/* Class to store minTemp and maxTemp and their counts */
	public static class MinMaxTemp implements Writable {

		private Double minTemp;
		private Double maxTemp;
		private Integer minCount = 1, maxCount = 1; // Initialized to one, if
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

	} // End of MinMaxTemp class

	// Mapper Class - emitting as emit(Text, MinMaxTemp)

	public static class TempMapper extends Mapper<Object, Text, Text, MinMaxTemp> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			MinMaxTemp mmt = new MinMaxTemp();
			Text stationId = new Text();
			String record = new String(value.toString());
			String token[] = record.split(",");

			// Checking for TMax and Tmin values
			if (token[2].equals("TMAX")) {
				stationId.set(token[0]);
				mmt.setMaxTemp(Double.parseDouble(token[3]));
				mmt.setMinTemp(-9999); // Updating MinTemp to -9999 for MaxTemp
										// values
				context.write(stationId, mmt);

			} else if (token[2].equals("TMIN")) {
				stationId.set(token[0]);
				mmt.setMinTemp(Double.parseDouble(token[3]));
				mmt.setMaxTemp(-9999); // Updating MinTemp to -9999 for MaxTemp
										// values

				context.write(stationId, mmt); // Emitting (Text-> StationId,
												// MinMaxTemp -> minTemp,
												// maxTemp,
												// minCount, maxCount)
			}
		}
	}

	// Combiner Class emitting the sum of minimum temperature, max temperature
	// and their counts

	public static class TempCombiner extends Reducer<Text, MinMaxTemp, Text, MinMaxTemp> {

		@Override
		public void reduce(Text key, Iterable<MinMaxTemp> values, Context context)
				throws IOException, InterruptedException {

			MinMaxTemp result = new MinMaxTemp();
			double minSum = 0, maxSum = 0;
			int minCount = 0, maxCount = 0;

			for (MinMaxTemp val : values) {// values has list of MinMaxTemp
											// objects

				if (val.getMinTemp() == -9999) { // If maximum temperature
					maxCount++; // incrementing counts
					maxSum += val.getMaxTemp(); // Summing Max Temp
				}
				if (val.getMaxTemp() == -9999) { // If minimum temperature
					minCount++; // Incrementing Counts
					minSum += val.getMinTemp(); // Summing min Temp
				}

			}

			if (minCount == 0)
				minSum = -9999; // Update min Sum to -9999 if all temperatures
								// are maximum
			if (maxCount == 0)
				maxSum = -9999; // Similar as for min Sum

			// Setting values in MinMaxTemp object
			result.setMinTemp(minSum);
			result.setMaxTemp(maxSum);
			result.setMinCount(minCount);
			result.setMaxCount(maxCount);

			context.write(key, result); // emit (Key -> StationId, result ->
										// minTemp, maxTemp, minCount, maxCount)
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
				} else { // MinMaxTemp object contains both min and max value
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

	// Driver Class

	// Driver Function
	
	public static void main(String ar[])throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"With Combiner");
		job.setJarByClass(WithCombiner.class);
		
		job.setMapperClass(TempMapper.class);
		job.setCombinerClass(TempCombiner.class); 	// Setting combiner class for this version
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