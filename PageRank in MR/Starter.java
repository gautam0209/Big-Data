package pageRank;

// This is starting class which will initiate 
// job to parse input files using AdjacencyList.java
// and PageRank.java to find top 100 page ranks.

public class Starter {

	public static void main(String ar[]) {
		try {
			long n = AdjacencyList.main(ar);
			String[] out = PageRank.main(n,ar);
			Top100.main(out);
		} catch (Exception e) { e.printStackTrace();
		}
	}
}
