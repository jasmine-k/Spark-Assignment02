package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataFrameOperations extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  private val sparkLogger = Logger.getLogger("spark")
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Football Matches Operations")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  /**
    * Reading the input CSV file using Spark Session and creating a Dataframe.
    */
  val filePath = "src/main/resources/D1.csv"
  val footballDF: DataFrame = sparkSession.read.option("header", true).option("inferSchema", true).csv(filePath)
  sparkLogger.info("Dataframe of football match: ")
  footballDF.show(false)

  /**
    * Total number of match played by each team as HOME TEAM
    */
  val nonzeroMatchHomeTeamDF = footballDF.groupBy("HomeTeam").count().withColumnRenamed("count", "Match count")
  val zeroMatchHomeTeamDF = footballDF.select("AwayTeam").except(footballDF.select("HomeTeam")).withColumn("Match Count", lit(0))
  val numberOfHomeMatchDF = nonzeroMatchHomeTeamDF.union(zeroMatchHomeTeamDF).toDF()
  sparkLogger.info("Total number of match played by each team as HOME TEAM : ")
  numberOfHomeMatchDF.show(false)

  /**
    * Top 10 team with highest winning percentage
    */
  val numberOfMatch: (Int, Int) => Int = (count1: Int, count2: Int) => count1 + count2
  val appendUDF = udf(numberOfMatch)
  val percentageOfWins: (Int, Int) => Double = (count1: Int, count2: Int) => (count1.toDouble / count2.toDouble) * 100
  val percentageOfWinsUDF = udf(percentageOfWins)

  val totalNumberOfMatch = footballDF.select($"HomeTeam").union(footballDF.select($"AwayTeam"))
    .groupBy($"HomeTeam").count().withColumnRenamed("count", "numberOfMatches")
  val countDF1 = footballDF.filter(row => row.get(6) == "H").groupBy("HomeTeam").count()
  val countDF2 = footballDF.filter(row => row.get(6) == "A").groupBy("AwayTeam").count()
  val winningDF = countDF1.join(countDF2, countDF1.col("HomeTeam") === countDF2.col("AwayTeam"), "outer")
    .withColumn("numberOfWins", appendUDF(countDF1.col("count"), countDF2.col("count")))
    .select($"HomeTeam", $"numberOfWins").withColumnRenamed("HomeTeam", "Team")

  val percentageOfWinDF = totalNumberOfMatch.join(winningDF, totalNumberOfMatch.col("HomeTeam") === winningDF.col("Team"))
    .withColumn("PercentageOfWins", percentageOfWinsUDF(winningDF.col("numberOfWins"), totalNumberOfMatch.col("numberOfMatches")))
    .select($"Team", $"PercentageOfWins").orderBy(desc("PercentageOfWins")).limit(10)
  sparkLogger.info("Top 10 team with highest wining percentage : ")
  percentageOfWinDF.show(false)

}
