package pagerankstart;

import java.io.StringReader;

/* XML Parser using Java
 * Will return nodes and its
 * edges in string
 */

import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class XMLJavaParser {
	
			private static Pattern namePattern;
			private static Pattern linkPattern;
			
			//private static StringBuilder dang = new StringBuilder();
			static {
				// Keep only html pages not containing tilde (~).
				namePattern = Pattern.compile("^([^~]+)$");
				// Keep only html filenames ending relative paths and not containing
				// tilde (~).
				linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
			}
		
			public static String parser(String line) {
				StringBuilder parent = new StringBuilder();
				try {
					// Configure parser.
					SAXParserFactory spf = SAXParserFactory.newInstance();
					spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
					SAXParser saxParser = spf.newSAXParser();
					XMLReader xmlReader = saxParser.getXMLReader();
					// Parser fills this list with linked page names.
					List<String> linkPageNames = new LinkedList<>();
					xmlReader.setContentHandler(new WikiParser(linkPageNames));
					// Each line formatted as (Wiki-page-name:Wiki-page-html).
				//	String line = value.toString();
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
								//dang.append(s + "\t" + "empty" + "\n");
								//context.write(new Text(s), new Text("empty"));
								parent.append(s + "|");
							}
							if (parent.length() > 0)
								parent.setLength(parent.length() - 1);

							// Sending pagename
							//context.write(new Text(pageName), new Text(sb.toString()));
							parent.append(pageName + "\t" + parent.toString());
						} catch (Exception e) {

						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return parent.toString();
				
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
