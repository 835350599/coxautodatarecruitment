package com.coxautodata.recruitment

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Areas {

  val rectangleSchema = StructType(Seq(
    StructField("length", IntegerType)
    , StructField("height", IntegerType)
  ))

  val circleSchema = StructType(Seq(
    StructField("radius", IntegerType)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val p = if (args.length == 0) {
      getClass.getResource("example.csv").getPath
    } else args(0)

    val a = calculateTotalArea(spark, p)

    println(a)
  }

  def calculateTotalArea(spark: SparkSession, path: String): Double = {

    val ds = spark.read.option("header", "true").option("escape", "\"").csv(path)

    rA(ds) + cA(ds)
  }

  def rA(df: DataFrame): Double = {
    import df.sparkSession.implicits._
    val rectangles = df.filter($"type" === "rectangle")
      .select(from_json($"details", rectangleSchema).as("rectangle"))

    rectangles.select(($"rectangle.length" * $"rectangle.height").as("area"))
      .agg(sum($"area")).as[Option[Double]].first().getOrElse(0)
  }

  def cA(df: DataFrame): Double = {
    import df.sparkSession.implicits._
    val circles = df.filter($"type" === "circle")
      .select(from_json($"details", circleSchema).as("circle"))

    circles.select(($"circle.radius" * $"circle.radius" * math.Pi).as("area"))
      .agg(sum($"area")).as[Option[Double]].first().getOrElse(0)
  }

}

