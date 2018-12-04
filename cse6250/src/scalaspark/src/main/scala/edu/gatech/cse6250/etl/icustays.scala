package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object ICUStayInfo {
  def getEarliestStays(icuStays: RDD[ICUStay]): RDD[FirstICUStay] = {
    icuStays.map(i => ((i.patientID, i.hadmID), i.inTime)).
      reduceByKey(Math.min).
      map { case ((pid, haid), inTime) => FirstICUStay(pid, haid, inTime) }
  }
}
