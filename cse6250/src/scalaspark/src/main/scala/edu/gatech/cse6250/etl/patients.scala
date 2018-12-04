package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD

object PatientInfo {
  case class PatientDetails(patientID: Long, hadmID: Long, gender: Int, age: Int)

  def getAges(patients: Dataset[Patient], admits: Dataset[AdmitEvent]): DataFrame = {
    val spark = patients.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val numMiliInYear = 365.2425 * 24 * 60 * 60 * 1000

    patients.join(admits, Seq("patientID"))
      .withColumn("age", ($"admitTime" - $"dob") / numMiliInYear)
  }

  def saveInfo(patients: Dataset[Patient], admits: Dataset[AdmitEvent]) {
    val spark = patients.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val p = patients.rdd
    val a = admits.rdd

    val numMiliInYear = 365.2425 * 24 * 60 * 60 * 1000

    val details = p.map(p => (p.patientID, p)).
      join(a.map(a => (a.patientID, a))).
      map {
        case (pid, (p, a)) => {
          PatientDetails(p.patientID, a.hadmID,
            p.gender match {
              case "f" => 1
              case _ => 0
            },
            Math.round((a.admitTime - p.dob).toFloat / numMiliInYear.toFloat))
        }
      }
    details.toDF.coalesce(1).write.option("header", true).csv("PATIENTDETAILS.csv")

  }
  def filterUnder18(patients: Dataset[Patient], admits: Dataset[AdmitEvent]): Dataset[PatientIDAdmitID] = {
    val spark = patients.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val withAges = getAges(patients, admits)

    withAges.filter(withAges("age") >= 18)
      .select($"patientID", $"hadmID")
      .as[PatientIDAdmitID]
  }

}
