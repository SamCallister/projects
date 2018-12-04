package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.utils.DateUtils
import edu.gatech.cse6250.helper.SparkHelper
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PulsePressureInfo {

  def getPulsePressure(vitals: RDD[Vital]): RDD[PulsePressure] = {
    val sysBP = vitals.filter(_.vitalID == Vital.SysBP.id).
      map(v => ((v.patientID, v.hadmID), v))
    val diasBP = vitals.filter(_.vitalID == Vital.DiasBP.id).
      map(v => ((v.patientID, v.hadmID), v))

    case class DifferenceEvent(difference: Double, timeDelta: Long)

    def nearestDifference(d1: DifferenceEvent, d2: DifferenceEvent): DifferenceEvent = {
      if (d1.timeDelta < d2.timeDelta) d1 else d2
    }

    val nearSysBP = sysBP.join(diasBP).
      map {
        case ((pid, haid), (s, d)) => {
          ((pid, haid, s.chartTime), DifferenceEvent(s.value - d.value, Math.abs(s.chartTime - d.chartTime)))
        }
      }.reduceByKey(nearestDifference).
      map {
        case ((pid, haid, stime), de) =>
          PulsePressure(pid, haid, stime, de.difference, de.timeDelta)
      }.
      filter(_.timeDelta <= DateUtils.DAYMILLIS)

    val nearDiasBP = diasBP.join(sysBP).
      map {
        case ((pid, haid), (d, s)) => {
          ((pid, haid, d.chartTime), DifferenceEvent(s.value - d.value, Math.abs(d.chartTime - s.chartTime)))
        }
      }.reduceByKey(nearestDifference).
      map {
        case ((pid, haid, dtime), de) =>
          PulsePressure(pid, haid, dtime, de.difference, de.timeDelta)
      }.
      filter(_.timeDelta <= DateUtils.DAYMILLIS)

    nearSysBP.union(nearDiasBP).distinct
  }
}
