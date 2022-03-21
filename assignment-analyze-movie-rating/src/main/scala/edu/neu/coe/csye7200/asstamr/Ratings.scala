package edu.neu.coe.csye7200.asstamr

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions, types}


object Ratings extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("AnalyzeMovieRating")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val movies: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/movie_metadata.csv")

  val movie_stats: DataFrame = movies.agg(functions.avg("imdb_score"), functions.stddev("imdb_score"))

  println(s"Average Movie Rating: ${movie_stats.collect()(0)(0)}")
  println(s"Standard Deviation of Movie Ratings: ${movie_stats.collect()(0)(1)}")
}
