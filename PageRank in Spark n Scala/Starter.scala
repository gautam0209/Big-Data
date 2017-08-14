package pagerankscala

// Starter Class to find Top100 webpages using PageRank Algorithms

object Starter {
  def main(ar : Array[String]) = {
		try {
		 	PageRank.main(ar)
		} catch {
		  case e: Exception => println(e.printStackTrace())
		}
	}
}   