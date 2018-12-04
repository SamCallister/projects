package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object FirstEventInfo {

  def getFirstEvents(chartevents: Dataset[ChartEvent]): DataFrame = {
    val spark = chartevents.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = chartevents.filter($"chartTime" > 0).groupBy("patientID", "hadmID")
      .agg(min("chartTime").alias("dateTime"))

    df
  }

}
