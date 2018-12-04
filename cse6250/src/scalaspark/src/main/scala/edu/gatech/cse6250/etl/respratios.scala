package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.utils.DateUtils
import edu.gatech.cse6250.helper.SparkHelper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RespiratoryRatioInfo {
  def getRespRatios(vitals: RDD[Vital]): RDD[SaO2FiO2Ratio] = {
    val saO2 = vitals.filter(_.vitalID == Vital.SaO2.id).
      map(v => ((v.patientID, v.hadmID), v))
    val fiO2 = vitals.filter(v => v.vitalID == Vital.FiO2.id && v.value > 0).
      map(v => ((v.patientID, v.hadmID), v))

    case class RatioEvent(ratio: Double, timeDelta: Long)

    def nearestRatio(r1: RatioEvent, r2: RatioEvent): RatioEvent = {
      if (r1.timeDelta < r2.timeDelta) r1 else r2
    }

    val nearSaO2 = saO2.join(fiO2).
      map {
        case ((pid, haid), (s, f)) => {
          ((pid, haid, s.chartTime), RatioEvent(100 * s.value / f.value, Math.abs(s.chartTime - f.chartTime)))
        }
      }.reduceByKey(nearestRatio).
      map {
        case ((pid, haid, stime), re) =>
          SaO2FiO2Ratio(pid, haid, stime, re.ratio, re.timeDelta)
      }.
      filter(_.timeDelta <= DateUtils.DAYMILLIS)

    val nearFiO2 = fiO2.join(saO2).
      map {
        case ((pid, haid), (f, s)) => {
          ((pid, haid, f.chartTime), RatioEvent(100 * s.value / f.value, Math.abs(f.chartTime - s.chartTime)))
        }
      }.reduceByKey(nearestRatio).
      map {
        case ((pid, haid, ftime), re) =>
          SaO2FiO2Ratio(pid, haid, ftime, re.ratio, re.timeDelta)
      }.
      filter(_.timeDelta <= DateUtils.DAYMILLIS)

    nearSaO2.union(nearFiO2).distinct
  }
}
