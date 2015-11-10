import java.util.Random
import scala.math
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object KMeans {

	def ClassifyPoint(p: (Double,Double), random:Array[(Double,Double)]): Int = {
    
	var index = 0
    var clusterId = 0
    var currentDistance = Double.PositiveInfinity

		for (i <- 0 to 2) 
		{
			var tempCtr = random(i)
			var tempDist = math.sqrt(math.pow(p._1.toDouble - tempCtr._1.toDouble,2)+ math.pow(p._2.toDouble - tempCtr._2.toDouble,2))
			if (tempDist < currentDistance) 
			{
				currentDistance = tempDist
				clusterId = i
			}
		}
		//println(random(clusterId))
		clusterId
	}

	def main(args: Array[String]) 
	{
		val logFile = "src/data/Q1_testkmean.txt" // Should be some file on your system
		val sc = new SparkContext("local", "K-Means App", "/path/to/spark-0.9.1-incubating",
		List("target/scala-2.10/simple-project_2.10-1.0.jar"))
		
		val logData = sc.textFile(logFile, 1).cache()
		val points = sc.textFile(logFile, 1).map(line=>line.split(" ")).map(line=>(line(0).toDouble,line(1).toDouble))
		
		// select random points
		var random =  points.takeSample(false, 3, 1987)

		//var random = Array((2.0,5.0),(1.0,4.0),(7.0,9.0))
		var itr = 0
		while(itr < 11)
		{
			var classifiedPoints = points.map(p=> (ClassifyPoint(p,random), (p,1.0)))
			
			var clusterCenters = classifiedPoints.reduceByKey((x,y)=>(((x._1._1 + y._1._1),(x._1._2 + y._1._2)),(x._2 + y._2)))
			
			var clusterCntersNew = clusterCenters.mapValues{ case (point, sum) => (point._1/sum,point._2/sum)}
			
			
			if( itr == 10 ){
				//random.foreach(println)
				clusterCntersNew.saveAsTextFile("src/data/Clusters")
				var clusterPoints = classifiedPoints.mapValues{ case (point,count) => (point._1,point._2)}
				clusterPoints.saveAsTextFile("src/data/ClusterPoints")
			}
			
			random = clusterCntersNew.values.toArray
			itr = itr + 1
			
		}
	}
}