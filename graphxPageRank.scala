import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}

object graphxPageRank {
  def main(args: Array[String]) {
      val startTime=System.currentTimeMillis()
      val inputFile = "hdfs://ec2-34-206-64-207.compute-1.amazonaws.com:9000/input-data"
      val outputFile = "hdfs://ec2-34-206-64-207.compute-1.amazonaws.com:9000/output-data"
      val iters = if (args.length > 1) args(1).toInt else 15
      val conf = new SparkConf()
      conf.setAppName("graphxpageRank")
      conf.setMaster("spark://ec2-34-206-64-207.compute-1.amazonaws.com:7077")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")
      
      val sparkC = new SparkContext(conf)

      val in = sparkC.textFile(inputFile)
      
      // Hash function to assign an Id to each article
      def pageHash(title: String): VertexId = {
        title.toLowerCase.replace(" ","").hashCode.toLong
      }
      
      var page = in.map(line => {
        val parts = line.split("\t")
        val (title, n) = (parts(1), parts(3).replace("\\n", "\n"))
        val links =
          if (n == "\\N") {
            NodeSeq.Empty
          } else {
            try {
              XML.loadString(n) \\ "link" \ "target"
            } catch {
              case e: org.xml.sax.SAXParseException =>
                System.err.println(" \"" + title + "\" has malformed XML in neighbour:\n" + n)
              NodeSeq.Empty
            }
          } 
        val outEdges = links.map(link => new String(link.text)).toArray
        val id = new String(title)    
        (id, outEdges)
      }).cache
      
      // Define vertices (page:RDD[(string, Array(string)])
      val vertices = page.map(tup => {
        val title = tup._1
        (pageHash(title), title)
      }).cache
      
      // Define edges (page:RDD[(string, Array(string)])
      val edges: RDD[Edge[Double]] = page.flatMap(tup => {
        val srcVid = pageHash(tup._1)
        val body = tup._2.iterator
          body.map(x => Edge(srcVid, pageHash(x), 1.0))
        }).cache

      val graph = Graph(vertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
      val prGraph=graph.staticPageRank(5).cache
      val titleAndPrGraph=graph.outerJoinVertices(prGraph.vertices){
			(v, title, rank)=>(rank.getOrElse(0.0), title)
		}
		  val result = titleAndPrGraph.vertices.top(100){
			Ordering.by((entry:(VertexId, (Double, String)))=>entry._2._1)
		}
		  val time=(System.currentTimeMillis()-startTime)/1000.0
		  sparkC.parallelize(result).map(t=>t._2._2+": "+t._2._1).saveAsTextFile(outputFile)
		  println("Completed in %f seconds: %f seconds/iteration".format(time, time/5))
	}
}