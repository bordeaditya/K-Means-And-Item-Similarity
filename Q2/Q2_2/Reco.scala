import java.util.Random
import scala.math
import scala.collection
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object Reco {
	
	def ReplaceKeyValue(key:String, values:String, movieIdTitle:Map[String,String]): String = {
   
		
		var value =""
		value = value + movieIdTitle(key) + "\t"
        //var  movieIdTitleArray= values._1.split(",")
		
		var  movieIdTitleArray= values.split(",")
		for (i <- 0 to movieIdTitleArray.length -1) {
			if(i < movieIdTitleArray.length -1)
			{
				value = value + movieIdTitle(movieIdTitleArray(i)) + ", "
			}
			else
			{
				value = value + movieIdTitle(movieIdTitleArray(i))
			}
		}
		
		value
		
	}
	
	def main(args: Array[String]) 
	{
		val logFileInput = "src/data/Mout" 
		val logFileMovies = "src/data/movies.dat"
		val logFileRating = "src/data/ratings.dat"
		val sc = new SparkContext("local", "Recommendation System", "/path/to/spark-0.9.1-incubating",
		List("target/scala-2.10/simple-project_2.10-1.0.jar"))
		
		//println("Enter the userId:")
		//var userId = readLine()
		
		var userId = "1"
		// Load movieId , userId and Rating =3
		val movieId_userId3 = sc.textFile(logFileRating).map(line=>line.split("::")).filter(line => line(2).equals("3")).filter(line => line(0).equals(userId)).map(line=>(line(1),""))
		
		
		//item similarity file
		val itemSimilarity = sc.textFile(logFileInput).map(line=>line.split("\t")).filter(line => (line.length >= 2)).map(line=>(line(0),line(1)))
		
		
		// Join the RDDs
		val joinedResult = itemSimilarity.join(movieId_userId3)
		
		// Result : 2
		val result_2 = joinedResult.map(line=>(line._1,line._2._1.replaceAll(" ",",")))
		
		// Get MovieId and Title
		val movieIdTitle = sc.textFile(logFileMovies).map(line=>line.split("::")).map(line=>(line(0),line(0)+" : "+line(1))).collectAsMap
		
		var result = result_2.map(line=> (ReplaceKeyValue(line._1,line._2,movieIdTitle.toMap)))
		
		// Final Result
		result.saveAsTextFile("src/data/Recommendation")
		
		
		//result.foreach(println)
		
		
	}
}