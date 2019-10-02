package com.coxautodata.recruitment

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

class TestAreas extends FunSpec with Matchers with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()
  }

  after {
    spark.stop()
  }

  describe("calculateTotalArea") {
    it("should calculate the total area for all shapes in the file") {
      Areas.calculateTotalArea(spark, getClass.getResource("test.csv").getPath) should be(487)
    }
  }
}
