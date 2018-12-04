package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD

object LabInfo {

  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/labs-first-day.sql
  def getSofaRelevantLabs(labResults: RDD[LabResult]): RDD[SofaLab] = {

    // We only care about Creatine, Bilirubin and Platelets
    // Sanity check the values
    labResults.filter((l: LabResult) => {
      l.itemID match {
        case 50912 => l.value <= 150 // CREATININE
        case 50885 => l.value <= 150 // BILIRUBIN
        case 51265 => l.value <= 10000 // PLATELET
        case _ => false
      }
    })
      .map((l: LabResult) => {
        SofaLab(l.patientID, l.hadmID, l.chartTime, l.itemID, l.value)
      })
  }
}