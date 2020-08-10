import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object GraphComponents {
  def main ( args: Array[String] ) {

		val conf=new SparkConf().setAppName("Project-8")
		val sc = new SparkContext(conf)
		val edges : RDD[Edge[String]] = sc.textFile(args(0)).map (
				(line) => {
							val fields = line.split(",");
							(fields(0), fields.slice(1, fields.length).toList)
				}
		).flatMap (
				(tup) => {
							tup._2.map( neighbour => (tup._1.toLong, neighbour.toLong) )
				}
		).map( pair => Edge(pair._1, pair._2, "")) // edges.collect() shows all the edges correctly
		
		val graph: Graph[Long, String] = Graph.fromEdges(edges, -1).mapVertices((id, v) => id) 
		
		val connectedComponents = graph.pregel(Long.MaxValue, 5) ( // Run 5 iterations
				  (id, group, newGroup) => math.min(group, newGroup), // Vertex Program
				  triplet => {  // Send Message
					if (triplet.srcAttr < triplet.dstAttr) {
					  Iterator((triplet.dstId, triplet.srcAttr.toInt))
					} else {
					  Iterator.empty
					}
				  },
				  (a, b) => math.min(a, b) // Merge Message
				)
				
		connectedComponents.vertices.map((k)=> (k._2,1)).reduceByKey(_+_).sortByKey().map((k) => {k._1+" "+k._2} ).collect.foreach(println) 
		
	
  }
}
