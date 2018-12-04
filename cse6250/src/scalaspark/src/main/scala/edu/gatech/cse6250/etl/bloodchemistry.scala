package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BloodChemistryInfo {
  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/blood-gas-first-day.sql
  def isBloodChem(itemID: Long): Boolean = {
    // 50807 is a commment
    ((50800L <= itemID) && (itemID <= 50806L)) ||
      ((50808L <= itemID) && (itemID <= 50828L))
  }

  def isValidValue(itemID: Long, value: Double): Boolean = {
    if (value < 0) {
      false
    } else {
      itemID match {
        case 50810 => (value <= 100.0) // hematocrit
        case 50816 => (value >= 20.0) && (value <= 100.0) // FIO2
        case 50817 => (value <= 100.0) // O2 sat
        case 50815 => (value <= 70.0) // O2 flow
        case 50821 => (value <= 800.0) // PO2
        case _ => true
      }
    }
  }

  def getBloodChemistryLabs(icuStays: RDD[ICUStay], labResults: RDD[LabResult]): RDD[BloodChemistry] = {
    val icuPairs = icuStays.map(i => ((i.patientID, i.hadmID), i))
    val labPairs = labResults.
      filter(l => isBloodChem(l.itemID) && isValidValue(l.itemID, l.value)).
      map(l => ((l.patientID, l.hadmID), l))

    val combined = icuPairs.join(labPairs)

    combined.map {
      case ((pid, haid), (icu, lr)) => {
        val chemID: Int = lr.itemID match {
          case 50800 => BloodChemistry.Specimen.id
          case 50801 => BloodChemistry.AaDO2.id
          case 50802 => BloodChemistry.BaseExcess.id
          case 50803 => BloodChemistry.Bicarbonate.id
          case 50804 => BloodChemistry.TotalCO2.id
          case 50805 => BloodChemistry.CarboxyHemoglobin.id
          case 50806 => BloodChemistry.Chloride.id
          case 50808 => BloodChemistry.Calcium.id
          case 50809 => BloodChemistry.Glucose.id
          case 50810 => BloodChemistry.Hematocrit.id
          case 50811 => BloodChemistry.Hemoglobin.id
          case 50812 => BloodChemistry.Intubated.id
          case 50813 => BloodChemistry.Lactate.id
          case 50814 => BloodChemistry.Methemoglobin.id
          case 50815 => BloodChemistry.O2Flow.id
          case 50816 => BloodChemistry.FIO2.id
          case 50817 => BloodChemistry.SO2.id
          case 50818 => BloodChemistry.PCO2.id
          case 50819 => BloodChemistry.PEEP.id
          case 50820 => BloodChemistry.PH.id
          case 50821 => BloodChemistry.PO2.id
          case 50822 => BloodChemistry.Potassium.id
          case 50823 => BloodChemistry.RequiredO2.id
          case 50824 => BloodChemistry.Sodium.id
          case 50825 => BloodChemistry.Temperature.id
          case 50826 => BloodChemistry.TidalVolume.id
          case 50827 => BloodChemistry.VentilationRate.id
          case 50828 => BloodChemistry.Ventilator.id
        }
        BloodChemistry(pid, haid, icu.icuStayID, lr.chartTime,
          chemID, lr.value, lr.abnormal)
      }
    }
  }
}
