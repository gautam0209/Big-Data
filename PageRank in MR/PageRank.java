package pageRank;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//This class will calculate the PageRank with handling dangling nodes

public class PageRank {

	static int itr = 0; // for iterations
	static int n; // numberofPages
	static double danglingSum = 0; // PageRank part by Dangling Node
	enum GC { DANGLING }; 

	// Mapper
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		
		public void setup(Context context)
		{
			Configuration conf = context.getConfiguration();
			n = Integer.parseInt(conf.get("n"));
			danglingSum = Double.parseDouble(conf.get("danglingSum"));
			itr = Integer.parseInt(conf.get("itr"));
			
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			double prob = 0, probOthers;
			String[] token = value.toString().split("\t");
			String values = new String();
			Text output = new Text();
			// for first iteration calculate initial page rank and append to
			// values
			if (itr == 1) {
				prob = 1 / (double) n;
				if (token.length == 2)
					values = prob + "|" + token[1];
				else
					values = Double.toString(prob);
			} else
				values = token[1];

			String[] val = values.toString().split("\\|");
			prob = Double.parseDouble(val[0]);

			double shareDangling = danglingSum / n; // share of Dangling
			double length = val.length;
			StringBuilder sb = new StringBuilder();
			probOthers = (prob * (1 / (length - 1))); // length -1 is number of
														// edges

			// emit each edge with its portion of pageRank
			for (int i = 1; i < length; i++) {
				context.write(new Text(val[i]), new Text(Double.toString(probOthers)));
				sb.append(val[i]);
				if (i != length - 1)
					sb.append("|");
			}

			// Emit the parent node with share of dangling node and its edges
			if (val.length == 1) // for dangling node
			{
				shareDangling = prob + shareDangling;
				output = new Text(Double.toString(shareDangling));

			} else // for normal nodes
				output = new Text(shareDangling + "|" + sb.toString());
			context.write(new Text(token[0]), output);

		}

		// cleanUp is called to initialize incoming page Rank to 0 before next
		// call to reduce
		protected void cleanup(Context con) {
			danglingSum = 0;
		}
	}

	// Reducer

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		TreeMap<Double, String> treeMap = new TreeMap<>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double sumProb = 0, pageRank = 0, d = 0, alpha = 0.15;
			String edges = new String();
			boolean dangling = true;
			StringBuilder sb = new StringBuilder();
			for (Text s : values) {
				try {
					d = Double.parseDouble(s.toString());
				}
				// exception will be thrown, if edges are there that means node
				// is not dangling
				catch (Exception e) {
					dangling = false;
					d = 0;
					String[] token = s.toString().split("\\|");
					if (token.length == 2) {
						pageRank += Double.parseDouble(token[0]); // for adding
																	// dangling
																	// share of
																	// previous
																	// reducer
						edges = token[1];
					} else
						edges = s.toString();
				}
				sumProb += d; // adding total pageRank for node
			}

			if (dangling) {
				danglingSum += sumProb; // calculating dangling share
				sumProb = 0;
			}

			pageRank = (alpha / n) + (1 - alpha) * sumProb; // computing
															// pageRank

			Text output = new Text(sb.append(pageRank + "|" + edges).toString());

			// for last iteration, write value into tree map
			if (itr == 10) {
				treeMap.put(pageRank, key.toString());
				if (treeMap.size() > 100) // if size is greater than 100, delete
											// the record
					treeMap.remove(treeMap.firstKey());
			} else
				context.write(key, output); // emitting [pagename
											// pageRank|listofedges]
		}

		// cleanup is called to set the danglingSum and to emit record in
		// files from treeMap
		// for last iteration
		protected void cleanup(Context context) throws InterruptedException, IOException {
			long counter = (long) (danglingSum * 1000000);
			context.getCounter(GC.DANGLING).increment(counter);
			if (itr == 10) {
				Iterator<Double> itr = treeMap.descendingKeySet().iterator();
				while (itr.hasNext()) {
					double key = itr.next();
					
					String pageName = treeMap.get(key);
					key += danglingSum/n; // For last share of dangling
					context.write(new Text(pageName), new Text(Double.toString(key)));
				}

			}

		}
	}

	// overloaded main function
	public static String[] main(long nodes, String[] ar) throws Exception {
		String input, output;
		output = new String();
		int itr =0;
		double danglingSum = 0;
		while (itr < 10) {
			if (itr == 0)
				input = ar[1];
			else 
				input = ar[1] + itr;
			output = ar[1] + ++itr;
			
			Configuration conf = new Configuration();
			
			conf.set("n", Long.toString(nodes) );
			conf.set("danglingSum", Double.toString(danglingSum));
			conf.set("itr", Integer.toString(itr));
			
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(PageRank.class);

			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path path = new Path(input);

			FileInputFormat.addInputPath(job, path);

			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

			// long counter to pass this among different job calls
			long counter = job.getCounters().findCounter(GC.DANGLING).getValue();
			danglingSum = (double) counter / 1000000; // converting back to
															// double

		}
		ar[0] = output;
		ar[1] = ar[1] + ++itr;
		return ar;
	}

}
