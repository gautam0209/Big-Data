package pagerankscala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import pagerankstart.XMLJavaParser


/* Scala Class to compute the pageRank 
 * It will read the .bz2 file line by line
 * and send it to XMLJavaParser and read the
 * parsed value and run the pageRanking algorithm 
 * on it.
 */

object PageRank {
  
  
  def main(ar : Array[String]) = {
          
     val results : StringBuilder = new StringBuilder()
    
     val conf = new SparkConf()
     
   //  val sc = new SparkContext(new SparkConf().setAppName("pageRank").setMaster("local"))
    val a = 0D
     //Spark Context
     val sc = new SparkContext(conf)
     val input = sc.textFile(ar(0))   //input command to read the bz2 file
     val inputArr = input.map(line => XMLJavaParser.parser(line)).persist()   //parse the xml file line by line
     
     // key,pair value for (nodes in edges, empty string)
     val pairInput = inputArr.flatMap{ar => {
       val arr = ar.split("\t")
       var one = new Array[String](1)
       var parent = arr(0)
       var edges = ""
       if(arr.length >1){
          edges  = arr(1) 
        var edgeNode = edges.split("\\|")
         for(e <- edgeNode) yield (e, "")}
       else {
         for(e <- one) yield (parent, "")
       }
       
     }
     }
     
     // key,value pair for (node, list of edges)
     val pairInput1 = inputArr.map{ ar =>
       val arr = ar.split("\t")
       var parent = arr(0)
       var edges = ""
       if (arr.length > 1)
         edges = arr(1)
       (parent,edges)
     }
       
       // combining key,value pair for both dangling and normal nodes
       val fullInput = pairInput.union(pairInput1)
   
       // reducing the keyValue pair
       val keyValPair = fullInput.reduceByKey(_+_)
       
       //count of nodes
       val l = fullInput.count()
       val n = l.toDouble
          
      // Initializing with pagRank
     val pageRank = keyValPair.mapValues(r => 1/n) //key,rank
     
     var i = 0
     
          // joining pagerank and edges
     var fullPair = keyValPair.join(pageRank).persist()
     
     // Running iteration for 10 times
     while(i<10)
     {
     // computing rankShare
     var rankShare = fullPair.values.flatMap{ case (edges, rank) =>
         val size = edges.split("\\|").size
         val bool = !edges.equals("")
         if(bool)
           edges.split("\\|").map( e => (e,rank/size))
         else
           edges.split("\\|").map( e => ("Dang",rank/size))
     }
     
     
     val nodeRank = fullPair.map(e => (e._1,0D))
     
     rankShare  = rankShare.++(nodeRank)
     
     
      // reducing rank share for all the keys
     val rankShareFin = rankShare.reduceByKey(_+_)
     
     
     
     //Computing pageRank using formula
     var finalRank = rankShareFin.map(r => (r._1 , ((a/n + (1-a) * r._2)))).persist()
       
     
     // Finding dangling sum for each node
     var listDanglingSum = finalRank.map(v => 
       if(v._1.equals("Dang"))
           (v._2)
       else
         0D)
         
         // total dangling sum
      var danglingSum = listDanglingSum.reduce(_+_)
       
      
     finalRank = finalRank.map(r => (r._1, (r._2 + danglingSum/n))) // adding dangling share
      
     //getting fullPair with finalRank and edges 
     fullPair = keyValPair.join(finalRank)
     
     i = i + 1
     }
       
     
     // Computing Top 100 WebPages
       
     val fullPairFin = fullPair.map(r => (r._2._2,r._1))
     
     //sorting webpages by rank in descending order
     val sortedPair = fullPairFin.sortByKey(false)
     
     
     val someRes = sortedPair.take(101)
     val parSomeRes = sc.parallelize(someRes)
     val outP = ar(1) + "100"
     
     //save top100 webpages
     parSomeRes.coalesce(1).saveAsTextFile(outP)     
  }
}