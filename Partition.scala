import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  type vertex = (Long,Long,List[Long])
  //var graph:List[vertex] = List[vertex]()
  val depth = 6

  def main ( args: Array[ String ] ) {

    val conf = new SparkConf().setAppName("partition")
    val sc = new SparkContext(conf)

    def getDataset(vertex: (Long, Long, List[Long])): (Long,(Long,List[Long])) ={
      (vertex._1,(vertex._2,vertex._3))
    }

    def assignGroupId(l: Long, l1: Long, longs: List[Long]): List[(Long, Long)] ={
      var res:List[(Long,Long)] = List[(Long,Long)]()
      res = (l,l1) :: res
      for(e <- longs){
        res = (e,l1) :: res
      }
      res
    }

    def getGraph(strings: Array[String], i: Int): vertex ={
      var id = strings(0).toLong
      var adj:List[Long] = List[Long]()
      for(j <- 1 until strings.length){
        adj = strings(j).toLong :: adj
      }
      if(i<5)
        (id,id,adj)
      else
        (id,-1,adj)
    }
    var count = -1
    var graph = sc.textFile(args(0)).map(line => {
      val a = line.split(",")
      count += 1
      getGraph(a,count)
    })

    for(i <- 1 to depth){
      graph = graph.flatMap{v => assignGroupId(v._1,v._2,v._3)}
                   .reduceByKey(_ max _)
                   .join(graph.map(res => getDataset(res)))
                   .map(g => {
                      if(g._2._2._1 == -1)
                        (g._1,g._2._1,g._2._2._2)
                      else
                        (g._1,g._2._2._1,g._2._2._2)
                    })
    }
    graph.map(c => (c._2,1))
      .reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}