package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class MatchOperations(HomeTeam: String, AwayTeam: String, FTHG: Int, FTAG: Int, FTR: String)

object DataSetOperations extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  private val sparkLogger = Logger.getLogger("spark")
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Football Matches Operations")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  /**
    * DataSet of football matches
    */
  val filePath = "src/main/resources/D1.csv"
  val footballDS = sparkSession.read.option("header", true).option("inferSchema", true).csv(filePath).
    select($"HomeTeam", $"AwayTeam", $"FTHG", $"FTAG", $"FTR").as[MatchOperations]
  sparkLogger.info("DataSet of football matches : ")
  footballDS.show(false)

  /**
    * Total number of match played by each team
    */
  val totalNumberOfMatch = footballDS.select($"HomeTeam").union(footballDS.select($"AwayTeam")).groupBy($"HomeTeam")
    .count().withColumnRenamed("count", "NumberOfMatches")
  sparkLogger.info("Total number of match played by each team ")
  totalNumberOfMatch.show(false)

  /**
    * Top Ten team with highest wins
    */
  val numberOfMatch: (Int, Int) => Int = (count1: Int, count2: Int) => count1 + count2
  val appendUDF = udf(numberOfMatch)
  val countWinAsHomeTeam = footballDS.filter(_.FTR == "H").groupBy("HomeTeam").count().withColumnRenamed("count", "count1")
  val countWinAsAwayTeam = footballDS.filter(_.FTR == "A").groupBy("AwayTeam").count().withColumnRenamed("count", "count2")
  val winningTeams = countWinAsHomeTeam.join(countWinAsAwayTeam, countWinAsHomeTeam.col("HomeTeam") === countWinAsAwayTeam.col("AwayTeam"), "outer")
    .withColumn("NumberOfWins", appendUDF($"count1", $"count2")).select($"HomeTeam", $"NumberOfWins").withColumnRenamed("HomeTeam", "Team").sort(desc("NumberOfWins")).limit(10)
  sparkLogger.info("Top Ten team with highest wins : ")
  winningTeams.show(false)

}
