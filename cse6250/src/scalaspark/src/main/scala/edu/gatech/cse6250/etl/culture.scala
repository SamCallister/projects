package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object CultureInfo {

  def getFirstCulture(cultures: Dataset[Culture]): DataFrame = {
    val spark = cultures.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    cultures.groupBy("patientID", "hadmID")
      .agg(min("time").alias("dateTime"))
  }

}