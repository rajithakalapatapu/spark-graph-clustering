import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6
  var count = 1

  def readInputFile(line: String): (Long, Long, List[Long]) = {
    var centroid: Long = -1

    val splitTokens = line.split(",")
    val vertexid = splitTokens(0).toLong

    if (count <= 5) {
      centroid = vertexid
      count += 1
    }

    var adjacent = splitTokens.drop(1).toList.map(token => { token.toLong })
    (
      vertexid,
      centroid,
      adjacent
    )
  }

  def chooseCentroid(
      tuple: (Long, (Long, (Long, List[Long])))
  ): (Long, Long, List[Long]) = {
    /* (3) */
    var finalCentroid: Long = -1L
    if (tuple._2._2._1 != -1L) {
      finalCentroid = tuple._2._2._1
    } else {
      finalCentroid = tuple._2._1
    }
    (tuple._1, finalCentroid, tuple._2._2._2)
  }

  def transformNode(
      node: (Long, Long, List[Long])
  ): (Long, (Long, List[Long])) = {
    /* (2) */
    (node._1, (node._2, node._3))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("partition")
    val sc = new SparkContext(conf)
    var countnew = 0
    /* read graph from args(0); the graph cluster ID is set to -1 except for the first 5 nodes */
    var graph = sc
      .textFile(args(0))
      .map(line => {
        val tokens = line.split(",")
        val vertexid = tokens(0).toLong
        val rest = tokens.drop(1)
        val adjacent = rest.map(token => {token.toLong}).toList
        var centroidFinal : Long = -1L
        if (countnew < 5) {
          centroidFinal = vertexid
          countnew += 1;
        } else {
          centroidFinal = -1
        }
        (vertexid, centroidFinal, adjacent)
      })
    /*
      for (i <- 1 to depth)
        graph = graph.flatMap{ /* (1) */ }
                      .reduceByKey(_ max _)
                      .join( graph.map( /* (2) */ ) )
                      .map{ /* (3) */ }
     */
    
    for (i <- 1 to depth) {
      graph = graph
        .flatMap {
          /* (1) */
          case (id, centroid, adjacent) =>
            (id, centroid)
            var x: List[Long] = adjacent.toList
            x.map(y => (y, centroid))
        }
        .reduceByKey(_ max _)
        .join(graph.map(node => transformNode(node)))
        .map { case (tuple) => chooseCentroid(tuple) }
    }

    /* finally, print the partition sizes */
    var partitionSizes = graph.map{
      case(id, centroid, adjacent) =>
      (centroid, 1)
    }.reduceByKey(_ + _)
    
    partitionSizes.collect.foreach(println)
  }
}