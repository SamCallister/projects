package edu.gatech.cse6250.data

import edu.gatech.cse6250.helper.{ CSVHelper, SparkHelper }
import org.apache.spark.sql.{ DataFrame, Dataset }
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.LabeledPoint

object DataSaver {
  val outputPath = "data/"
  val vitalsPath = outputPath + "VITALEVENTS.csv"
  val labsPath = outputPath + "SOFALABS.csv"
  val comaScoresPath = outputPath + "COMASCORES.csv"
  val bloodChemistryPath = outputPath + "BLOODCHEMISTRY.csv"
  val weightPath = outputPath + "PATIENTWEIGHTSFROMNOTES.csv"
  val urineInfoPath = outputPath + "URINE.csv"
  val firstEventsPath = outputPath + "FIRSTEVENTS.csv"
  val sofaWindowPath = outputPath + "SOFAWINDOW.csv"
  val weightsForPatientsPath = outputPath + "PATIENTWEIGHTS.csv"
  val saO2fiO2RatioPath = outputPath + "SAO2FIO2EVENTS.csv"
  val pulsePressurePath = outputPath + "PULSEPRESSUREEVENTS.csv"
  val vasopressorsPath = outputPath + "VASOPRESSORS.csv"
  val sofaFeaturesPath = outputPath + "SOFAFEATURES.csv"
  val sofaScorePath = outputPath + "SOFASCORES.csv"
  val patientLabelsPath = outputPath + "PATIENTLABELS.csv"
  val featuresPath = outputPath + "FEATURES.csv"

  def saveVitals(vitals: RDD[Vital], spark: SparkSession): Unit = {
    import spark.implicits._
    vitals.toDF.write.option("header", false).csv(vitalsPath)
  }

  def saveSofaLabs(sofaLabs: RDD[SofaLab], spark: SparkSession): Unit = {
    import spark.implicits._
    sofaLabs.toDF.coalesce(1).write.option("header", true).csv(labsPath)
  }

  def saveComaScores(comaScores: RDD[ComaScore], spark: SparkSession): Unit = {
    import spark.implicits._
    comaScores.toDF.coalesce(1).write.option("header", true).csv(comaScoresPath)
  }

  def saveBloodChemistry(bloodChemistry: RDD[BloodChemistry], spark: SparkSession): Unit = {
    import spark.implicits._
    bloodChemistry.toDF.coalesce(1).write.option("header", true).csv(bloodChemistryPath)
  }

  def savePatientWeightsFromNotes(weights: RDD[Weight], spark: SparkSession): Unit = {
    import spark.implicits._
    weights.toDF.coalesce(1).write.option("header", true).csv(weightPath)
  }

  def saveUrineInfo(urineInfo: RDD[UrineOutputEvent], spark: SparkSession): Unit = {
    import spark.implicits._
    urineInfo.toDF.coalesce(1).write.option("header", true).csv(urineInfoPath)
  }

  def saveFirstEvents(firstEvents: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    firstEvents.coalesce(1).write.option("header", true).csv(firstEventsPath)
  }

  def saveSofaWindow(sofaWindowFrame: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    sofaWindowFrame.coalesce(1).write.option("header", true).csv(sofaWindowPath)
  }

  def savePatientWeights(weights: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    weights.coalesce(1).write.option("header", true).csv(weightsForPatientsPath)

  }

  def saveSaO2FiO2Ratios(ratios: RDD[SaO2FiO2Ratio], spark: SparkSession): Unit = {
    import spark.implicits._
    ratios.toDF.write.option("header", false).csv(saO2fiO2RatioPath)
  }

  def savePulsePressure(pressures: RDD[PulsePressure], spark: SparkSession): Unit = {
    import spark.implicits._
    pressures.toDF.write.option("header", false).csv(pulsePressurePath)
  }

  def saveVasopressors(vasopressors: RDD[Vasopressor], spark: SparkSession): Unit = {
    import spark.implicits._
    vasopressors.toDF.write.option("header", false).csv(vasopressorsPath)
  }

  def saveSofaFeatures(sofaFeatures: RDD[SofaFeatures], spark: SparkSession): Unit = {
    import spark.implicits._
    sofaFeatures.toDF.write.option("header", false).csv(sofaFeaturesPath)
  }

  def saveSofaScores(sofaScores: RDD[SofaScore], spark: SparkSession): Unit = {
    import spark.implicits._
    sofaScores.toDF.write.option("header", false).csv(sofaScorePath)
  }

  def savePatientLabels(patientLabels: RDD[LabelEvent], spark: SparkSession): Unit = {
    import spark.implicits._
    patientLabels.toDF.coalesce(1).write.option("header", true).csv(patientLabelsPath)
  }

  def saveFeatures(features: RDD[Features], spark: SparkSession): Unit = {
    import spark.implicits._
    features.toDF.write.option("header", true).csv(featuresPath)
  }

  def saveAggregatedFeatures(windowMax: Int, aggFeatures: RDD[ModelFeatures], spark: SparkSession): Unit = {
    import spark.implicits._
    val path = outputPath + "features" + windowMax.toString
    aggFeatures.toDF.write.option("header", false).csv(path)
  }
}
