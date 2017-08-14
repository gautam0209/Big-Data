package pageRank;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class AdjacencyList {

	enum GC{ NUMBEROFNODES}; // Global counter to return total no of nodes

	// A reducer class is used just to emit the nodes which are present in edges
	// but not present as parent node
	public static class WikiReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.getCounter(GC.NUMBEROFNODES).increment(1);
			Text edges = new Text();
			for (Text s : values) {
				if (!s.toString().equals("empty")) {
					edges = s;
					break;
				}
			}

			context.write(key, edges);

		}

	}

	// Mapper will parse the input file and emit the pageName and list of
	// pageNames in edges
	public static class WikiMapper extends Mapper<Object, Text, Text, Text> {
		private static Pattern namePattern;
		private static Pattern linkPattern;
		static {
			// Keep only html pages not containing tilde (~).
			namePattern = Pattern.compile("^([^~]+)$");
			// Keep only html filenames ending relative paths and not containing
			// tilde (~).
			linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		}

		public void map(Object key, Text value, Context context) {
			try {
				// Configure parser.
				StringBuilder sb = new StringBuilder();
				SAXParserFactory spf = SAXParserFactory.newInstance();
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				SAXParser saxParser = spf.newSAXParser();
				XMLReader xmlReader = saxParser.getXMLReader();
				// Parser fills this list with linked page names.
				List<String> linkPageNames = new LinkedList<>();
				xmlReader.setContentHandler(new WikiParser(linkPageNames));
				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				String line = value.toString();
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				html = html.replace("&","&amp;");
				Matcher matcher = namePattern.matcher(pageName);
				if (matcher.find()) {

					// Parse page and fill list of linked pages.
					linkPageNames.clear();
					try {
						xmlReader.parse(new InputSource(new StringReader(html)));
						for (String s : linkPageNames) {
							// sending nodes for edges - to look for dangling in
							// Reduce
							context.write(new Text(s), new Text("empty"));
							sb.append(s + "|");
						}
						if (sb.length() > 0)
							sb.setLength(sb.length() - 1);

						// Sending pagename
						context.write(new Text(pageName), new Text(sb.toString()));
					} catch (Exception e) {

					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/** Parses a Wikipage, finding links inside bodyContent div element. */
		public static class WikiParser extends DefaultHandler {
			/** List of linked pages; filled by parser. */
			private List<String> linkPageNames;
			/** Nesting depth inside bodyContent div element. */
			private int count = 0;

			public WikiParser() {
			}

			public WikiParser(List<String> linkPageNames) {
				super();
				this.linkPageNames = linkPageNames;
			}

			@Override
			public void startElement(String uri, String localName, String qName, Attributes attributes)
					throws SAXException {
				super.startElement(uri, localName, qName, attributes);
				if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id"))
						&& count == 0) {
					// Beginning of bodyContent div element.
					count = 1;
				} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
					// Anchor tag inside bodyContent div element.
					count++;
					String link = attributes.getValue("href");
					if (link == null) {
						return;
					}
					try {
						// Decode escaped characters in URL.
						link = URLDecoder.decode(link, "UTF-8");
					} catch (Exception e) {
						// Wiki-weirdness; use link as is.
					}
					// Keep only html filenames ending relative paths and not
					// containing tilde (~).
					Matcher matcher = linkPattern.matcher(link);
					if (matcher.find()) {
						linkPageNames.add(matcher.group(1));
					}
				} else if (count > 0) {
					// Other element inside bodyContent div.
					count++;
				}
			}

			@Override
			public void endElement(String uri, String localName, String qName) throws SAXException {
				super.endElement(uri, localName, qName);
				if (count > 0) {
					// End of element inside bodyContent div.
					count--;
				}
			}
		}

	}

	// It will return the total number of nodes in input file

	public static long main(String ar[]) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Adj List");
		job.setJarByClass(AdjacencyList.class);

		job.setMapperClass(WikiMapper.class);
		job.setReducerClass(WikiReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(ar[0]));
		FileOutputFormat.setOutputPath(job, new Path(ar[1]));
		job.waitForCompletion(true);

		return job.getCounters().findCounter(GC.NUMBEROFNODES).getValue();

	}
}
