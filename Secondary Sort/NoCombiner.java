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

/* Class with no combiner */

public class NoCombiner {

	/* Class to store minTemp and maxTemp value */
	public static class MinMaxTemp implements Writable {

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

	} // End of MinMaxTemp Class

	// Mapper Class - emitting as emit(Text, MinMaxTemp)

	public static class TempMapper extends Mapper<Object, Text, Text, MinMaxTemp> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			MinMaxTemp mmt = new MinMaxTemp();
			Text stationId = new Text();
			String record = new String(value.toString());
			String token[] = record.split(",");

			// Checking for TMax and TMin
			if (token[2].equals("TMAX")) {
				stationId.set(token[0]);
				mmt.setMaxTemp(Double.parseDouble(token[3]));
				mmt.setMinTemp(-9999); // Updating min temp to -9999 for Tmax
										// values
				context.write(stationId, mmt);
			} else if (token[2].equals("TMIN")) {
				stationId.set(token[0]);
				mmt.setMinTemp(Double.parseDouble(token[3]));
				mmt.setMaxTemp(-9999); // Updating max temp to -9999 for Tmin
										// Values

				context.write(stationId, mmt); // Emitting (Text, MinMaxTemp->
												// minTemp,maxTemp)
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

			for (MinMaxTemp val : values) { // values has list of MinMaxTemp
											// objects

				if (val.getMinTemp() == -9999) { // if Maximum Temp
					maxCount++;
					maxSum += val.getMaxTemp(); // Summing Up the Maximum
												// Temperatures
				}
				if (val.getMaxTemp() == -9999) { // if Minimum Temp
					minCount++;
					minSum += val.getMinTemp(); // Summing up Minimum
												// Temperatures
				}

			}

			// Calculating Min and Max Average
			minAvg = minSum / minCount;
			maxAvg = maxSum / maxCount;

			result.setMinTemp(minAvg);
			result.setMaxTemp(maxAvg);
			context.write(key, result); // Emitting (key-> StationId, result ->
										// mintempAvg, maxtempAvg)
		}
	}

	// Driver Function
	
	public static void main(String ar[])throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"No Combiner");
		job.setJarByClass(NoCombiner.class);
		
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