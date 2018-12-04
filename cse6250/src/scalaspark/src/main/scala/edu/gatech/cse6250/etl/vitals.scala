package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object VitalInfo {
  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/vitals-first-day.sql
  // FiO2 -- https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/blood-gas-first-day-arterial.sql
  // modified to include SaO2 and FiO2
  val HEARTRATE = Set(211L, 220045L)
  val SYSBP = Set(51L, 442L, 455L, 6701L, 220179L, 220050L)
  val DIASBP = Set(8368L, 8440L, 8441L, 8555L, 220180L, 220051L)
  val MEANBP = Set(456L, 52L, 6702L, 443L, 220052L, 220181L, 225312L)
  val RESPRATE = Set(618L, 615L, 220210L, 224690L)
  val SPO2 = Set(646L, 220277L)
  val SAO2 = Set(834L, 220227L)
  val FIO2 = Set(3420L, 190L, 223835, 3422)
  val GLUCOSE = Set(807L, 811L, 1529L, 3745L, 3744L, 225664L, 220621L, 226537L)
  val TEMPC = Set(226537L, 676L)
  val TEMPF = Set(223761L, 678L)
  val VITALS = HEARTRATE.union(SYSBP).union(DIASBP).union(MEANBP).union(RESPRATE).
    union(SPO2).union(SAO2).union(FIO2) union (GLUCOSE).union(TEMPC).union(TEMPF)

  def isValidValue(itemID: Long, value: Double): Boolean = {
    if (SAO2(itemID)) {
      (value > 0.0) && (value <= 100.0)
    } else if (HEARTRATE(itemID)) {
      (value > 0.0) && (value < 300.0)
    } else if (SYSBP(itemID)) {
      (value > 0.0) && (value < 400.0)
    } else if (DIASBP(itemID) || MEANBP(itemID)) {
      (value > 0.0) && (value < 300.0)
    } else if (RESPRATE(itemID)) {
      (value > 0.0) && (value < 70.0)
    } else if (TEMPF(itemID)) {
      (value > 70.0) && (value < 120.0)
    } else if (TEMPC(itemID)) {
      (value > 10.0) && (value < 50.0)
    } else if (SPO2(itemID)) {
      (value > 0.0) && (value < 100.0)
    } else if (GLUCOSE(itemID)) {
      value > 0.0
    } else {
      // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/blood-gas-first-day-arterial.sql
      // Make sure FiO2 values are in proper ranges
      itemID match {
        case 223835 => ((value > 0.0) && (value <= 1.0) ||
          (value >= 21.0))
        case 190 => (value > 0.20) && (value <= 1.0)
        case _ => true
      }
    }
  }

  def getVitals(chartEvents: RDD[ChartEvent]): RDD[Vital] = {
    chartEvents.
      filter(!_.error).
      filter(c => VITALS(c.itemID) && isValidValue(c.itemID, c.value)).
      map(ce => {
        if (HEARTRATE(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.HeartRate.id, ce.value)
        } else if (SYSBP(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.SysBP.id, ce.value)
        } else if (DIASBP(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.DiasBP.id, ce.value)
        } else if (MEANBP(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.MeanBP.id, ce.value)
        } else if (RESPRATE(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.RespRate.id, ce.value)
        } else if (SPO2(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.SpO2.id, ce.value)
        } else if (SAO2(ce.itemID)) {
          val adjustedValue = if (ce.value <= 1.0) ce.value * 100 else ce.value
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.SaO2.id, adjustedValue)
        } else if (FIO2(ce.itemID)) {
          val adjustedValue = ce.itemID match {
            case 223835 => {
              if (ce.value <= 1.0) ce.value * 100
              else ce.value
            }
            case 190 => { ce.value * 100 }
            case _ => ce.value
          }
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.FiO2.id, adjustedValue)
        } else if (GLUCOSE(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.Glucose.id, ce.value)
        } else if (TEMPC(ce.itemID)) {
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.TempC.id, ce.value)
        } else {
          // TempF -- convert to C
          Vital(ce.patientID, ce.hadmID, ce.icuStayID, ce.chartTime, Vital.TempC.id,
            (ce.value - 32.0) / 1.8)
        }
      })
  }
}
