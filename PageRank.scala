import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml.{XML,NodeSeq}

object PageRank {
   def main(args: Array[String]) {
	val startTime=System.currentTimeMillis
      val inputFile = "hdfs://ec2-34-206-64-207.compute-1.amazonaws.com:9000/input-data"
      val outputFile = "hdfs://ec2-34-206-64-207.compute-1.amazonaws.com:9000/output-data"
      val iters = if (args.length > 1) args(0).toInt else 2
      val conf = new SparkConf()
      conf.setAppName("pureSparkpageRank")
      conf.setMaster("spark://ec2-34-206-64-207.compute-1.amazonaws.com:7077")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      
      val sparkC = new SparkContext(conf)
      
      val in = sparkC.textFile(inputFile)
      
      var page = in.map(line => {
        val parts = line.split("\t")
        val (title, n) = (parts(0), parts(3).replace("\\n", "\n"))
        val links =
          if (n == "\\N") {
            NodeSeq.Empty
          } else {
            try {
              XML.loadString(n) \\ "link" \ "target"
            } catch {
              case e: org.xml.sax.SAXParseException =>
                System.err.println(" \"" + title + "\" has malformed XML in neighbour\n" + n)
              NodeSeq.Empty
            }
          } 
        val outEdges = links.map(link => new String(link.text)).toArray
        val id = new String(title)    
        (id, outEdges)
      }).cache
      var ranks = page.mapValues(v => 1.0)
      for (i <- 1 to iters) {
        val contributes = page.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
          }  
        ranks = contributes.reduceByKey(_+_).mapValues (0.15 + 0.85 * _)
      }
      val out = ranks.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
      sparkC.parallelize(out).saveAsTextFile(outputFile)
	val time=(System.currentTimeMillis-startTime)/1000.0	
     ranks.collect().foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
      sparkC.stop()
	println("Completed in %f seconds: %f seconds/iteration".format(time, time/15))
    }   
}