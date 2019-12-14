import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Partition {

  def main(args: Array[String] ) {

    val conf = new SparkConf().setAppName("graph_partition")
    val sc = new SparkContext(conf)

    def getEdges(strings: Array[String],i:Long): List[(Long,Long,Long)] ={
      var adj: List[(Long,Long,Long)] = List[(Long,Long,Long)]()
      for(j <- strings.indices){
        adj = (strings(0).toLong,strings(j).toLong,i) :: adj
      }
      adj
    }

    var count:Long = 0
    val edgesInGraph = sc.textFile(args(0)).map(line => {
      val a = line.split(",")
      count += 1
      getEdges(a,count)
    }).flatMap(e =>{
      e.map(ed =>{
        Edge(ed._1,ed._2,ed._3)
      })
    })

    val graph = Graph.fromEdges(edgesInGraph,1L)
    val initialClusters = graph.edges.filter(e => e.attr <= 5).map(e=>{e.srcId}).distinct().collect()
    val ic = sc.broadcast(initialClusters)
    val newGraph = graph.mapVertices((id,_) => {
      if(ic.value.contains(id))
        id
      else
        -1L
    })


    val finalGraph = newGraph.pregel(-1L,maxIterations = 6)(
      (_,attr,newAttr)=> {
        if(attr == -1L)
          newAttr
        else
          attr
      },
      triplet => {
        Iterator((triplet.dstId,triplet.srcAttr))
      },
      (a,b)=> Math.max(a,b)
    )

    finalGraph.vertices.map(c => (c._2,1))
      .reduceByKey(_+_).collect().foreach(println)

    sc.stop()

  }
}
