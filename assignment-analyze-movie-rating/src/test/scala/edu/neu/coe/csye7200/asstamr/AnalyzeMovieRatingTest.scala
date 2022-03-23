package edu.neu.coe.csye7200.asstamr

import com.phasmidsoftware.parse.RawParsers.WithHeaderRow.RawTableParser
import com.phasmidsoftware.table.Table
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest.tagobjects.Slow

import scala.util.Success

class AnalyzeMovieRatingTest extends AnyFlatSpec with Matchers with BeforeAndAfter{

  implicit var spark: SparkSession = _

    before {
      spark = SparkSession
        .builder()
        .appName("AnalyzeMovieRating")
        .master("local[*]")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
    }

    after {
      if (spark != null) {
        spark.stop()
      }
    }

  behavior of "parseResource"

  it should "read movie_metadata.csv successfully" in {
    Table.parseResource("/movie_metadata.csv", getClass) should matchPattern { case Success(_) => }
  }

  behavior of "computeAverageRating"

  it should "successfully return 6.453200745804848 for the mean" taggedAs Slow in {
    RatingsAnalyzer.computeAverageRating should matchPattern {
      case 6.435290363752392 =>
    }
  }

  behavior of "computeStdDeviationRating"
  it should "successfully return 0.9988071293753289 for the standard deviation" taggedAs Slow in {
    RatingsAnalyzer.computeStdDevRating should matchPattern {
      case 0.9928346233517373 =>
    }
  }

}
