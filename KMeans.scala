import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def main(args: Array[ String ]) {
    /* ... */
    def distance(pt:Point,c:Point): Double ={
      val first = Math.pow((pt._1 - c._1),2)
      val second = Math.pow((pt._2 - c._2),2)
      Math.sqrt(first+second)
    }
    def findAvg(pt:Iterable[Point]): Point ={
      var sumX = 0.0
      var sumY = 0.0
      pt.foreach(p => {sumX += p._1
        sumY +=p._2})
      (sumX/pt.size,sumY/pt.size)
    }
    val conf = new SparkConf().setAppName("kmeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line =>
                { val a = line.split(",")
                  (a(0).toDouble,a(1).toDouble) } )
                .collect()

    val points = sc.textFile(args(0)).map(line =>{
      val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)
    })

    for ( i <- 1 to 5 ) {

      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p, _)), p) }
        .groupByKey().map { res => findAvg(res._2) }
          .collect()
    }
    centroids.foreach(println)
    sc.stop()
  }
}
