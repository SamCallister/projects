package edu.gatech.cse6250.main

import edu.gatech.cse6250.etl._
import edu.gatech.cse6250.helper.{ CSVHelper, SparkHelper }
import edu.gatech.cse6250.data.{ DataLoader, DataSaver }
import edu.gatech.cse6250.prediction.RFModel
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  val USAGE = """
Usage: <mode> [args]
  
where <mode> is one of:
  
  <general dataset modes>
  inspect                (print out a couple values from standard datasets)
  inspectVitals          (print out a couple values from vital dataset)
  saveAggregatedfeatures (save features for initial classification attempt)
  saveBloodChemistry     (save blood chemistry dataset)
  saveFirstevents        (save details of first events)
  saveNoteevents         (save selected events from chart notes)
  
  <benchmark dataset modes>
  saveComaScores         (save coma scores benchmark dataset)
  saveLabs               (save benchmark labs dataset)
  saveUrine              (save benchmark urine output events dataset)
  saveRespRatios         (save benchmark SaO2:FiO2 events dataset)
  savePulsePressures     (save benchmark pulse pressures dataset)
  saveVasopressors       (save benchmark vasopressors dataset)
  saveSofaFeatures       (save benchmark sofafeatures dataset)
  saveVitals             (save benchmark vitals dataset)
  savePatientWeights     (save benchmark weights dataset)
  
  <cohort/label datasets>
  saveSofaScores         (save sofa scores dataset)
  savePatientDetails     (save patient ages and genders dataset)
  savePatientLabels      (save cohort with labels septic/non-septic)
  savePatientSofaWindows (save cohort windows for computing sofa scores)
  
  <prediction modes>
  trainModel dataPath modelPath (train Random Forest model and save to filesystem)
  runModel modelPath dataPath   (run saved Random Forest model on test data)
"""

  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length == 0) {
      println(USAGE)
      return
    }

    val mode = args(0)

    mode match {
      case "inspect" => inspect
      case "saveVitals" => saveVitals
      case "saveLabs" => saveLabs
      case "inspectVitals" => inspectVitals
      case "saveComaScores" => saveComaScores
      case "saveBloodChemistry" => saveBloodChemistry
      case "saveNoteEvents" => saveNoteEvents
      case "saveUrine" => saveUrine
      case "savePatientSofaWindows" => savePatientSofaWindows
      case "saveFirstEvents" => saveFirstEvents
      case "savePatientWeights" => savePatientWeights
      case "saveRespRatios" => saveRespRatios
      case "savePulsePressures" => savePulsePressures
      case "saveSofaFeatures" => saveSofaFeatures
      case "saveVasopressors" => saveVasopressors
      case "saveSofaScores" => saveSofaScores
      case "savePatientLabels" => savePatientLabels
      case "saveModelFeatures" => saveModelFeatures
      case "saveAggregatedFeatures" => saveAggregatedFeatures
      case "trainModel" => trainModel(args)
      case "runModel" => runModel(args)
      case "savePatientDetails" => savePatientDetails
      case _ => println(USAGE)
    }
  }

  def inspectVitals: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val vitals = DataLoader.loadVitals(spark).rdd
    vitals.take(2).foreach(println)

    val saO2Count = vitals.filter(_.vitalID == Vital.SaO2.id).count
    val fiO2Count = vitals.filter(_.vitalID == Vital.FiO2.id).count
    val saO2Min = vitals.filter(_.vitalID == Vital.SaO2.id).map(_.value).min
    val saO2Max = vitals.filter(_.vitalID == Vital.SaO2.id).map(_.value).max
    println(s"SaO2 count = $saO2Count")
    println(s"SaO2 min = $saO2Min")
    println(s"SaO2 max = $saO2Max")
    println(s"FiO2 count = $fiO2Count")
  }

  def inspect: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val cultures = DataLoader.loadCultures(spark).rdd
    val dItems = DataLoader.loadDItems(spark).rdd
    val patients = DataLoader.loadPatients(spark)
    val mvInputEvents = DataLoader.loadMVInputEvents(spark).rdd
    val cvInputEvents = DataLoader.loadCVInputEvents(spark)
    val chartEvents = DataLoader.loadChartEvents(spark, false).rdd
    val prescriptions = DataLoader.loadPrescriptions(spark)
    val icuStays = DataLoader.loadICUStays(spark).rdd
    val labResults = DataLoader.loadLabResults(spark)
    val outputEvents = DataLoader.loadOutputEvents(spark)

    chartEvents.take(2).foreach(println)
    cultures.take(2).foreach(println)
    dItems.take(2).foreach(println)
    mvInputEvents.take(2).foreach(println)
    cvInputEvents.take(2).foreach(println)
    patients.take(2).foreach(println)
    prescriptions.take(2).foreach(println)
    icuStays.take(2).foreach(println)
    labResults.take(2).foreach(println)
    outputEvents.take(2).foreach(println)

    sc.stop
  }

  def saveVitals: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val chartEvents = DataLoader.loadChartEvents(spark, false).rdd
    val vitals = VitalInfo.getVitals(chartEvents)
    println("Saving vitals to " + DataSaver.vitalsPath)
    DataSaver.saveVitals(vitals, spark)

    sc.stop()
  }

  def saveLabs: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val labResults = DataLoader.loadLabResults(spark)
    val sofaLabs = LabInfo.getSofaRelevantLabs(labResults)

    print("Saving labs to " + DataSaver.labsPath)
    DataSaver.saveSofaLabs(sofaLabs, spark)

    sc.stop()
  }

  def saveComaScores: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val chartEvents = DataLoader.loadChartEvents(spark, true).rdd

    val comaScores = ComaScoreInfo.getComaScores(chartEvents)

    print("Saving coma scores to " + DataSaver.comaScoresPath)
    DataSaver.saveComaScores(comaScores, spark)

    sc.stop()
  }

  def saveBloodChemistry: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val labResults = DataLoader.loadLabResults(spark)

    val icuStays = DataLoader.loadICUStays(spark).rdd

    val bloodChemistry = BloodChemistryInfo.getBloodChemistryLabs(icuStays, labResults)

    println("Saving blood chemistry info to " + DataSaver.bloodChemistryPath)
    DataSaver.saveBloodChemistry(bloodChemistry, spark)

    sc.stop
  }

  def saveNoteEvents: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val noteEvents = DataLoader.loadNoteEvents(spark)
    val weights = WeightInfo.getPatientWeightsFromNotes(noteEvents)

    println("Saving weights to " + DataSaver.weightPath)
    DataSaver.savePatientWeightsFromNotes(weights, spark)

    sc.stop
  }

  def saveUrine: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    val icuStays = DataLoader.loadICUStays(spark).rdd
    val outputEvents = DataLoader.loadOutputEvents(spark)

    val urineInfo = UrineInfo.getUrineInfo(icuStays, outputEvents)

    println("Saving urine info to " + DataSaver.urineInfoPath)
    DataSaver.saveUrineInfo(urineInfo, spark)

    sc.stop()
  }

  def saveFirstEvents: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val chartEvents = DataLoader.loadChartEvents(spark, false)
    val firstEvents = FirstEventInfo.getFirstEvents(chartEvents)

    println("Saving first events to " + DataSaver.firstEventsPath)

    DataSaver.saveFirstEvents(firstEvents, spark)

    sc.stop()
  }

  def savePatientSofaWindows: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val firstEvents = DataLoader.loadFirstEvents(spark)

    val prescriptions = DataLoader.loadPrescriptions(spark)
    val cvInputsDf = DataLoader.loadCVInputEvents(spark)
    val mvInputsDf = DataLoader.loadMVInputEvents(spark)
    val dItems = DataLoader.loadDItems(spark)
    val firstAntiBioticTimes = AntibioticInfo.getFirstAntibioticDate(prescriptions, cvInputsDf, mvInputsDf, dItems)

    val cultures = DataLoader.loadCultures(spark)
    val firstCultureTimes = CultureInfo.getFirstCulture(cultures)

    val icuStays = DataLoader.loadICUStays(spark)
    val windowTimes = SofaWindowInfo.getWindowTimes(firstAntiBioticTimes, firstCultureTimes, firstEvents, icuStays)

    val admits = DataLoader.loadAdmissions(spark)
    val patients = DataLoader.loadPatients(spark)

    val adults = PatientInfo.filterUnder18(patients, admits)

    val filteredWindowTimes = SofaWindowInfo.removeChildrenFromWindowTimes(windowTimes, adults)

    println("Saving window times to " + DataSaver.sofaWindowPath)

    DataSaver.saveSofaWindow(filteredWindowTimes, spark)

    sc.stop()
  }

  def savePatientWeights: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val chartEvents = DataLoader.loadChartEvents(spark, true)
    val weightsFromNotes = DataLoader.loadWeightsFromNotes(spark)

    val weightsForPatients = WeightInfo.combineWeightsFromEchoAndChartEvents(chartEvents, weightsFromNotes)

    println("Saving weights for patients " + DataSaver.weightsForPatientsPath)

    DataSaver.savePatientWeights(weightsForPatients, spark)

    sc.stop()
  }

  def saveRespRatios: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val vitals = DataLoader.loadVitals(spark).rdd
    val ratios = RespiratoryRatioInfo.getRespRatios(vitals)
    println("Saving ratios to " + DataSaver.saO2fiO2RatioPath)
    DataSaver.saveSaO2FiO2Ratios(ratios, spark)

    sc.stop()
  }

  def savePulsePressures: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val vitals = DataLoader.loadVitals(spark).rdd
    val pressures = PulsePressureInfo.getPulsePressure(vitals)
    println("Saving ratios to " + DataSaver.pulsePressurePath)
    DataSaver.savePulsePressure(pressures, spark)

    sc.stop()
  }

  def saveVasopressors: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val inputEventsCv = DataLoader.loadCVInputEvents(spark).rdd
    val inputEventsMv = DataLoader.loadMVInputEvents(spark).rdd
    val avgWeights = DataLoader.loadAverageWeights(spark)
    val vasopressors = VasopressorInfo.getVasopressors(inputEventsCv, inputEventsMv, avgWeights)
    println("Saving vasopressors to " + DataSaver.vasopressorsPath)
    DataSaver.saveVasopressors(vasopressors, spark)

    sc.stop()
  }

  def saveSofaFeatures: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val vasopressors = DataLoader.loadVasopressors(spark)
    val comaScores = DataLoader.loadComaScores(spark).rdd
    val vitals = DataLoader.loadVitals(spark).rdd
    val urineOutput = DataLoader.loadUrineOutput(spark)
    val sofaLabs = DataLoader.loadSofaLabs(spark)
    val respRatios = DataLoader.loadSaO2FiO2Events(spark)

    val sofaFeatures = SofaFeaturesInfo.getAggregatedFeatures(
      vitals,
      sofaLabs,
      urineOutput,
      comaScores,
      respRatios,
      vasopressors)
    println("Saving sofa features to " + DataSaver.sofaFeaturesPath)
    DataSaver.saveSofaFeatures(sofaFeatures, spark)

    sc.stop
  }

  def saveSofaScores: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val sofaFeatures = DataLoader.loadSofaFeatures(spark)
    val sofaScores = SofaScoreInfo.getSofaScores(sofaFeatures)
    println("Saving sofa scores to " + DataSaver.sofaScorePath)
    DataSaver.saveSofaScores(sofaScores, spark)

    sc.stop
  }

  def savePatientLabels: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val sofaScores = DataLoader.loadSofaScores(spark)
    val sofaWindows = DataLoader.loadSofaWindows(spark)
    val labeledPatients = LabelInfo.labelPatients(sofaScores, sofaWindows)

    println("Num with sepsis")
    println(labeledPatients.filter(l => l.isSeptic > 0).count)
    println("Num without sepsis")
    println(labeledPatients.filter(l => l.isSeptic == 0).count)

    println("Saving Patient Labels to " + DataSaver.patientLabelsPath)
    DataSaver.savePatientLabels(labeledPatients, spark)

    sc.stop
  }

  def saveModelFeatures: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    val vitals = DataLoader.loadVitals(spark)
    val comaScores = DataLoader.loadComaScores(spark)
    val pulsePressures = DataLoader.loadPulsePressures(spark)
    val labels = DataLoader.loadLabels(spark)

    val modelFeatures = FeatureInfo.getFeatures(vitals, comaScores, pulsePressures, labels)

    println("Saving Model Features to " + DataSaver.featuresPath)
    DataSaver.saveFeatures(modelFeatures, spark)

    sc.stop
  }

  def savePatientDetails: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    val admits = DataLoader.loadAdmissions(spark)
    val patients = DataLoader.loadPatients(spark)
    val withAges = PatientInfo.saveInfo(patients, admits)
    sc.stop
  }

  def trainModel(args: Array[String]): Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    if (args.length != 3) {
      println("Usage: trainModel <svmlight> <modelToSave>")
    } else {
      RFModel.fitAndSave(args(1), args(2))
    }
    sc.stop
  }

  def runModel(args: Array[String]): Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext

    if (args.length != 3) {
      println("Usage: runModel <savedModel> <svmlight>")
    } else {
      RFModel.loadAndPredict(args(1), args(2))
    }
    sc.stop
  }

  def saveAggregatedFeatures: Unit = {
    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    val vitals = DataLoader.loadVitals(spark)
    val comaScores = DataLoader.loadComaScores(spark)
    val pulsePressures = DataLoader.loadPulsePressures(spark)
    val windows = DataLoader.loadSofaWindows(spark)
    val labels = DataLoader.loadLabels(spark)

    for (i <- List.range(1, 43)) {
      val hour = i * 12
      val points = ModelFeaturesInfo.aggregateFeatures(hour, vitals.rdd, comaScores.rdd,
        pulsePressures.rdd, windows, labels.rdd)
      println(s"Saving features up to hour $hour")
      DataSaver.saveAggregatedFeatures(hour, points, spark)
    }
    sc.stop
  }
}
