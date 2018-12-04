package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object SofaWindowInfo {

  def getWindowTimes(firstAntibiotic: DataFrame, firstCulture: DataFrame, firstEvent: DataFrame, icuStays: Dataset[ICUStay]): DataFrame = {
    val spark = firstAntibiotic.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val fourtyEightHoursInMiliSeconds = 48 * 60 * 60 * 1000
    val twentyFourHoursInMiliSeconds = 24 * 60 * 60 * 1000

    val icuDepatures = icuStays.filter($"outTime" > 0)
      .groupBy("patientID", "hadmID")
      .agg(max("outTime").alias("dateTime"))

    val firstCultureOrAnti = firstAntibiotic.union(firstCulture)
      .groupBy("patientID", "hadmID")
      .agg(min("dateTime").alias("dateTime"))

    val dayAfterFirstCultureOrAnti = firstCultureOrAnti
      .withColumn("dateTime", $"dateTime" + twentyFourHoursInMiliSeconds)

    val windowEnd = icuDepatures.union(dayAfterFirstCultureOrAnti)
      .groupBy("patientID", "hadmID")
      .agg(max("dateTime").alias("windowEnd"))

    val twoDaysBeforeFirstCultureOrAnti = firstCultureOrAnti
      .withColumn("dateTime", $"dateTime" - fourtyEightHoursInMiliSeconds)

    val windowStart = twoDaysBeforeFirstCultureOrAnti
      .union(firstEvent)
      .groupBy("patientID", "hadmID")
      .agg(max("dateTime").alias("windowStart"))

    windowStart.join(windowEnd, windowStart("patientID") === windowEnd("patientID") && windowStart("hadmID") === windowEnd("hadmID"))
      .select(windowStart("patientID"), windowStart("hadmID"), windowStart("windowStart"), windowEnd("windowEnd"))
      .filter($"windowStart" < $"windowEnd")
  }

  def removeChildrenFromWindowTimes(windows: DataFrame, adultPatients: Dataset[PatientIDAdmitID]) = {
    windows.join(adultPatients, Seq("patientID", "hadmID"))
  }

}
