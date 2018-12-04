package edu.gatech.cse6250.model

case class Patient(patientID: Long, gender: String, dob: Long, dod: Long,
  dodHosp: Long, dodSSN: Long, died: Boolean)

object Gender extends Enumeration {
  val Male, Female = Value
}

case class Culture(patientID: Long, hadmID: Long, time: Long,
  specID: Long, description: String)

case class DItem(itemID: Long, label: String, source: String, linksTo: String,
  category: String)

case class InputEvent(patientID: Long, hadmID: Long, icuStayID: Long,
  startTime: Long, endTime: Long, itemID: Long, amount: Double, rate: Double)

case class ICUStay(patientID: Long, hadmID: Long, icuStayID: Long, source: String,
  firstUnit: String, lastUnit: String, firstWardID: Long, lastWardID: Long,
  inTime: Long, outTime: Long, stayLength: Double)

case class FirstICUStay(patientID: Long, hadmID: Long, inTime: Long)

case class ChartEvent(patientID: Long, hadmID: Long, icuStayID: Long, itemID: Long,
  chartTime: Long, value: Double, error: Boolean, valueString: String)

case class Prescription(patientID: Long, hadmID: Long, icuStayID: Long,
  startDate: Long, endDate: Long, medicine: String)

case class LabResult(patientID: Long, hadmID: Long, itemID: Long,
  chartTime: Long, value: Double, abnormal: Boolean)

case class OutputEvent(patientID: Long, hadmID: Long, icuStayID: Long,
  chartTime: Long, itemID: Long, amount: Double)

case class NoteEvent(patientID: Long, hadmID: Long, chartTime: Long, text: String)

case class Weight(patientID: Long, hadmID: Long, chartTime: Long, weight: Double)

object Vital extends Enumeration {
  val HeartRate, SysBP, DiasBP, MeanBP, RespRate, TempC, SpO2, Glucose, SaO2, FiO2 = Value
}

case class Vital(patientID: Long, hadmID: Long, icuStayID: Long, chartTime: Long,
  vitalID: Int, value: Double)

case class SofaLab(patientID: Long, hadmID: Long, chartTime: Long, itemID: Long, value: Double)

case class ComaScore(patientID: Long, hadmID: Long, chartTime: Long, score: Double)

object BloodChemistry extends Enumeration {
  val Specimen, AaDO2, BaseExcess, Bicarbonate, TotalCO2, CarboxyHemoglobin, Chloride, Calcium, Glucose, Hematocrit, Hemoglobin, Intubated, Lactate, Methemoglobin, O2Flow, FIO2, SO2, PCO2, PEEP, PH, PO2, Potassium, RequiredO2, Sodium, Temperature, TidalVolume, VentilationRate, Ventilator = Value
}

case class BloodChemistry(patientID: Long, hadmID: Long, icuStayID: Long, chartTime: Long,
  chemistryID: Int, value: Double, abnormal: Boolean)

case class UrineOutputEvent(patientID: Long, hadmID: Long, icuStayID: Long, chartTime: Long, amount: Double)

case class FirstAntiBioticDate(patientID: Long, hadmID: Long, icuStayID: Long, chartTime: Long)

// Provides the features aggregated by hour in order to compute SOFA score
// (Use -1.0 for any missing feature)
case class SofaFeatures(patientID: Long, hadmID: Long, chartTime: Long,
  minRespRatio: Double, minPlatelets: Double, maxBilirubin: Double, minMeanBP: Double,
  maxNorepinephrine: Double, maxEpinephrine: Double, maxDopamine: Double, maxDobutamine: Double,
  minGCS: Double, maxCreatinine: Double, netUrine: Double) {
  def withMinRespRatio(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, value,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMinPlatelets(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      value, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMaxBilirubin(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, value, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMinMeanBP(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, value,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMaxNorepinephrine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      value, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withEpinephrine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, value, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMaxEpinephrine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, value, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMaxDopamine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, value, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMaxDobutamine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, value,
      this.minGCS, this.maxCreatinine, this.netUrine)
  }
  def withMinGCS(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      value, this.maxCreatinine, this.netUrine)
  }
  def withMaxCreatinine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, value, this.netUrine)
  }
  def withNetUrine(value: Double): SofaFeatures = {
    SofaFeatures(this.patientID, this.hadmID, this.chartTime, this.minRespRatio,
      this.minPlatelets, this.maxBilirubin, this.minMeanBP,
      this.maxNorepinephrine, this.maxEpinephrine, this.maxDopamine, this.maxDobutamine,
      this.minGCS, this.maxCreatinine, value)
  }
}

case class SaO2FiO2Ratio(patientID: Long, hadmID: Long, chartTime: Long, ratio: Double, timeDelta: Long)

case class AverageWeight(patientID: Long, hadmID: Long, avgWeight: Double)

case class PulsePressure(patientID: Long, hadmID: Long, chartTime: Long, value: Double, timeDelta: Long)

object Vasopressor extends Enumeration {
  val Epinephrine, Norepinephrine, Dopamine, Dobutamine = Value
}

case class Vasopressor(patientID: Long, hadmID: Long, icuStayID: Long, startTime: Long, endTime: Long,
  vpID: Int, amount: Double, rate: Double)

// Provide sofa score by hour for each patient.
case class SofaScore(patientID: Long, hadmID: Long, chartTime: Long, score: Int,
  respScore: Int, renalScore: Int, neuroScore: Int, cardioScore: Int, liverScore: Int, coagScore: Int)

case class SofaWindow(patientID: Long, hadmID: Long, windowStart: Long, windowEnd: Long)

// Label a patient as septic or not.
// Not sure if we need icuStayID yet.
// onsetDelta is the number of hours from the "first event" at which patient becomes septic
// We still have to define what is the first event.
// 1: isSeptic, 0: not septic
case class LabelEvent(patientID: Long, hadmID: Long, onsetDelta: Int, timeLabeledSeptic: Long, windowStart: Long, isSeptic: Int)

// Speculative definition for model feature for predictions.
// May change. Just ideas to get started.
// I think we don't need ICU stay for this, because we consider a span that
// may include multiple ICU stays.
// Add in age?
case class OptionFeatures(patientID: Long, hadmID: Long, chartTime: Long,
  minSystolicBP: Option[Double], maxSystolicBP: Option[Double],
  minPulsePressure: Option[Double], maxPulsePressure: Option[Double],
  minHeartRate: Option[Double], maxHeartRate: Option[Double],
  minRespRate: Option[Double], maxRespRate: Option[Double],
  minTempC: Option[Double], maxTempC: Option[Double],
  minSpO2: Option[Double],
  minGCS: Option[Double],
  hourDelta: Int,
  isSeptic: Int) // from first event

case class Features(patientID: Long, hadmID: Long, chartTime: Long,
  minSystolicBP: Double, maxSystolicBP: Double,
  minPulsePressure: Double, maxPulsePressure: Double,
  minHeartRate: Double, maxHeartRate: Double,
  minRespRate: Double, maxRespRate: Double,
  minTempC: Double, maxTempC: Double,
  minSpO2: Double,
  minGCS: Double,
  hourDelta: Int,
  isSeptic: Int)

object ModelFeature extends Enumeration {
  val MinSystolicBP, MaxSystolicBP, MinHeartRate, MaxHeartRate, MinRespRate, MaxRespRate, MinTempC, MaxTempC, MinSpO2, MinPulsePressure, MaxPulsePressure, MinGCS, Age, PrevProbability = Value
}

case class AggregatedModelVitals(patientID: Long, hadmID: Long,
  minSystolicBP: Double, maxSystolicBP: Double,
  minHeartRate: Double, maxHeartRate: Double,
  minRespRate: Double, maxRespRate: Double,
  minTempC: Double, maxTempC: Double,
  minSpO2: Double)

case class AggregatedModelPressures(patientID: Long, hadmID: Long, minPulsePressure: Double, maxPulsePressure: Double)

case class AggregatedModelComaScores(patientID: Long, hadmID: Long, minComaScore: Double)

case class ModelFeatures(patientID: Long, hadmID: Long,
  minSystolicBP: Double, maxSystolicBP: Double,
  minHeartRate: Double, maxHeartRate: Double,
  minRespRate: Double, maxRespRate: Double,
  minTempC: Double, maxTempC: Double,
  minSpO2: Double,
  minPulsePressure: Double, maxPulsePressure: Double,
  minGCS: Double,
  previousProbability: Double,
  isSeptic: Int)

case class AdmitEvent(patientID: Long, hadmID: Long, admitTime: Long)

case class PatientIDAdmitID(patientID: Long, hadmID: Long)
