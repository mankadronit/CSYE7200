package edu.neu.coe.csye7200.asstamr

import com.phasmidsoftware.table.Table
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import scala.util.{Failure, Success, Try}

case class RatingsAnalyzer(resource: String) {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AnalyzeMovieRating")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import MovieParser._
  import spark.implicits._

  val dy: Try[Dataset[Movie]] = Table.parseResource(resource) map (mt => spark.createDataset(mt.rows.toSeq))

  def parseDataset(ds: Try[Dataset[Movie]]): Dataset[Movie] = {
    Try(ds.get) match {
      case Success(s) => s
      case Failure(e) => throw e
    }
  }

  val ds: Dataset[Movie] = parseDataset(dy)

  val computeAverageRating: Any = ds.select(ds.col("reviews.imdbScore")).agg(functions.avg("imdbScore")).collect()(0)(0)
  val computeStdDevRating: Any = ds.select(ds.col("reviews.imdbScore")).agg(functions.stddev("imdbScore")).collect()(0)(0)
}

object RatingsAnalyzer extends RatingsAnalyzer("/movie_metadata.csv") {

  def main(args:Array[String]): Unit = {
    println(s"Average of Movie Ratings: ${RatingsAnalyzer.computeAverageRating}")
    println(s"Std Dev of Movie Ratings: ${RatingsAnalyzer.computeStdDevRating}")
  }
}
