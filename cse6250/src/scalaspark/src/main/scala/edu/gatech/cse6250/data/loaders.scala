package edu.gatech.cse6250.data

import org.joda.time.format.DateTimeFormat;
import edu.gatech.cse6250.helper.{ CSVHelper, SparkHelper }
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, DataFrame, SparkSession }

object DataLoader {
  val dataPath = "data/"
  val culturePath = dataPath + "MICROBIOLOGYEVENTS.csv"
  val patientsPath = dataPath + "PATIENTS.csv"
  val dItemsPath = dataPath + "D_ITEMS.csv"
  val mvInputEventsPath = dataPath + "INPUTEVENTS_MV.csv"
  val cvInputEventsPath = dataPath + "INPUTEVENTS_CV.csv"
  val icuStaysPath = dataPath + "ICUSTAYS.csv"
  val chartEventsPath = dataPath + "CHARTEVENTS.csv"
  val prescriptionsPath = dataPath + "PRESCRIPTIONS.csv"
  val labResultsPath = dataPath + "LABEVENTS.csv"
  val outputEventsPath = dataPath + "OUTPUTEVENTS.csv"
  // Below are our "homemade" datasets
  val vitalsPath = dataPath + "VITALEVENTS.csv"
  val bloodChemistryPath = dataPath + "BLOODCHEMISTRY.csv"
  val noteEventsPath = dataPath + "NOTEEVENTS.csv"
  val firstEventsPath = dataPath + "FIRSTEVENTS.csv"
  val weightsFromNotesPath = dataPath + "PATIENTWEIGHTSFROMNOTES.csv"
  val saO2fiO2RatioPath = dataPath + "SAO2FIO2EVENTS.csv"
  val pulsePressuresPath = dataPath + "PULSEPRESSUREEVENTS.csv"
  val avgPatientWeightsPath = dataPath + "PATIENTWEIGHTS.csv"
  val vasopressorsPath = dataPath + "VASOPRESSORS.csv"
  val comaScoresPath = dataPath + "COMASCORES.csv"
  val urineOutputPath = dataPath + "URINE.csv"
  val sofaFeaturesPath = dataPath + "SOFAFEATURES.csv"
  val sofaLabsPath = dataPath + "SOFALABS.csv"
  val sofaScoresPath = dataPath + "SOFASCORES.csv"
  val sofaWindowPath = dataPath + "SOFAWINDOW.csv"
  val admissionsDataPath = dataPath + "ADMISSIONS.csv"
  val patientLabelsPath = dataPath + "PATIENTLABELS.csv"

  val sharedDateTimeFormat = SparkHelper.spark.
    sparkContext.broadcast(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
  val sharedDateFormat = SparkHelper.spark.
    sparkContext.broadcast(DateTimeFormat.forPattern("yyyy-MM-dd"))
  val sharedDateHourFormat = SparkHelper.spark.
    sparkContext.broadcast(DateTimeFormat.forPattern("yyyy-MM-dd HH"))

  /**
   *  Parse a timestamp from a string, falling back to a reference
   *  date if provided.
   *
   *  @param timeStamp the timestamp to parse
   *  @param date a reference date, in case timeStamp is blank
   *  @return the time in milliseconds
   */
  def getTimestamp(timeStamp: String, date: String = ""): Long = {
    // Some datafiles (like MICROBIOLOGYEVENTS) provide a chart date AND
    // a chart time.  If the chart time is provided, it includes the date.
    // If the chart time is not provided, fall back to the chart date.
    if (timeStamp.nonEmpty) {
      // Tim: removed try/catch.  Dunno if there are instances of broken
      // formats in the files.  Apologies for any NPEs.
      sharedDateTimeFormat.value.parseDateTime(timeStamp).getMillis
    } else {
      if (date.nonEmpty) {
        sharedDateTimeFormat.value.parseDateTime(date).getMillis
      } else {
        -1
      }
    }
  }

  /**
   * Extract a date quantized to hour from a string.
   * May be useful for aggregating chart events by hour.
   *
   * @param value the date string
   * @param unused an unused parameter to make the signature comparable with getTimeStamp
   * @return the time in milliseconds
   */
  def getDateAndHour(value: String, unused: String = ""): Long = {
    if (value.isEmpty) {
      -1
    } else {
      sharedDateHourFormat.value.parseDateTime(value.substring(0, 13)).getMillis
    }
  }

  def getDateAndHourNoteEvents(value: String): Long = {
    if (value.isEmpty) {
      -1
    } else if (value.length >= 13) {
      sharedDateHourFormat.value.parseDateTime(value.substring(0, 13)).getMillis
    } else {
      sharedDateFormat.value.parseDateTime(value).getMillis
    }
  }

  def loadPatients(spark: SparkSession): Dataset[Patient] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, patientsPath)
    val patients = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, GENDER,
        |IFNULL(DOB, ''), 
        |IFNULL(DOD, ''),
        |IFNULL(DOD_HOSP, ''),
        |IFNULL(DOD_SSN, ''),
        |EXPIRE_FLAG
        |FROM PATIENTS
      """.stripMargin).
      map(r =>
        Patient(
          r(0).toString.toLong,
          r(1).toString.toLowerCase,
          getTimestamp(r(2).toString.trim),
          getTimestamp(r(3).toString.trim),
          getTimestamp(r(4).toString.trim),
          getTimestamp(r(5).toString.trim),
          r(6).toString.trim == "1"))
    patients
  }

  def loadCultures(spark: SparkSession): Dataset[Culture] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, culturePath)
    val cultures = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |HADM_ID,
        |CHARTDATE,
        |IFNULL(CHARTTIME, ''),
        |SPEC_ITEMID,
        |SPEC_TYPE_DESC
        |FROM MICROBIOLOGYEVENTS 
        |WHERE (CHARTDATE IS NOT NULL) AND (SPEC_ITEMID IS NOT NULL)
      """.stripMargin).
      map(r =>
        Culture(
          r(0).toString.toLong,
          r(1).toString.toLong,
          getTimestamp(r(3).toString.trim, r(2).toString.trim),
          r(4).toString.toLong,
          r(5).toString.trim.toLowerCase))
    cultures
  }

  def loadDItems(spark: SparkSession): Dataset[DItem] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, dItemsPath)
    val ditems = sqlContext.sql(
      """
        |SELECT ITEMID,
        |LABEL,
        |DBSOURCE, 
        |IFNULL(LINKSTO, ''),
        |IFNULL(CATEGORY, '')
        |FROM D_ITEMS 
        |WHERE LABEL IS NOT NULL
      """.stripMargin).
      map(r =>
        DItem(
          r(0).toString.toLong,
          r(1).toString.trim.toLowerCase,
          r(2).toString.trim.toLowerCase,
          r(3).toString.trim.toLowerCase,
          r(4).toString.trim.toLowerCase))
    ditems
  }

  def loadMVInputEvents(spark: SparkSession): Dataset[InputEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, mvInputEventsPath)
    val events = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |HADM_ID,
        |IFNULL(ICUSTAY_ID, '-1'),
        |STARTTIME,
        |ENDTIME,
        |ITEMID, AMOUNT, IFNULL(RATE, '-1.0')
        |FROM INPUTEVENTS_MV
      """.stripMargin).
      map(r =>
        InputEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          getTimestamp(r(3).toString.trim),
          getTimestamp(r(4).toString.trim),
          r(5).toString.toLong,
          r(6).toString.toDouble,
          r(7).toString.toDouble))
    events
  }

  def loadCVInputEvents(spark: SparkSession): Dataset[InputEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, cvInputEventsPath)
    val events = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |IFNULL(HADM_ID, '-1'),
        |IFNULL(ICUSTAY_ID, '-1'),
        |CHARTTIME,
        |ITEMID,
        |IFNULL(AMOUNT, '-1'),
        |IFNULL(RATE, '-1.0')
        |FROM INPUTEVENTS_CV
      """.stripMargin).
      map(r =>
        InputEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          getTimestamp(r(3).toString.trim),
          -1,
          r(4).toString.toLong,
          r(5).toString.toDouble,
          r(6).toString.toDouble))
    events
  }

  def loadChartEvents(spark: SparkSession, quantizeByHour: Boolean = false): Dataset[ChartEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext
    val getTime: (String, String) => Long = if (quantizeByHour) getDateAndHour else getTimestamp

    CSVHelper.loadCSVAsTable(spark, chartEventsPath)
    val events = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |HADM_ID,
        |IFNULL(ICUSTAY_ID, '-1'),
        |IFNULL(ITEMID, 'MISSING'),
        |CHARTTIME,
        |VALUENUM,
        |IFNULL(ERROR, ''),
        |IFNULL(VALUE, '')
        |FROM CHARTEVENTS
        |WHERE (CHARTTIME IS NOT NULL) AND (ITEMID != 'MISSING') AND (VALUENUM IS NOT NULL)
      """.stripMargin).
      map(r =>
        ChartEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          r(3).toString.toLong,
          getTime(r(4).toString.trim, ""),
          r(5).toString.toDouble,
          r(6).toString.trim == "1",
          r(7).toString))
    events
  }

  def loadPrescriptions(spark: SparkSession): Dataset[Prescription] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    def getDrugName(drug: String, poe: String, generic: String): String = {
      // Prefer GENERIC name.
      // If generic not available, try POE and then finally DRUG.
      if (generic.nonEmpty) {
        generic.toLowerCase
      } else if (poe.nonEmpty) {
        poe.toLowerCase
      } else if (drug.nonEmpty) {
        drug.toLowerCase
      } else {
        ""
      }
    }

    CSVHelper.loadCSVAsTable(spark, prescriptionsPath)
    val prescriptions = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |HADM_ID,
        |IFNULL(ICUSTAY_ID, '-1'),
        |IFNULL(STARTDATE, ''),
        |IFNULL(ENDDATE, ''),
        |IFNULL(DRUG, ''),
        |IFNULL(DRUG_NAME_POE, ''),
        |IFNULL(DRUG_NAME_GENERIC, '')
        |FROM PRESCRIPTIONS
      """.stripMargin).
      map(r =>
        Prescription(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          getTimestamp(r(3).toString),
          getTimestamp(r(4).toString),
          getDrugName(
            r(5).toString.trim,
            r(6).toString.trim,
            r(7).toString.trim)))
    prescriptions
  }

  def loadICUStays(spark: SparkSession): Dataset[ICUStay] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, icuStaysPath)
    val icuStays = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |HADM_ID,
        |ICUSTAY_ID,
        |DBSOURCE,
        |FIRST_CAREUNIT,
        |LAST_CAREUNIT,
        |FIRST_WARDID,
        |LAST_WARDID,
        |INTIME,
        |IFNULL(OUTTIME, ''),
        |IFNULL(LOS, '-1.0')
        |FROM ICUSTAYS
      """.stripMargin).
      map(r =>
        ICUStay(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          r(3).toString.trim.toLowerCase,
          r(4).toString.trim.toLowerCase,
          r(5).toString.trim.toLowerCase,
          r(6).toString.toLong,
          r(7).toString.toLong,
          getTimestamp(r(8).toString),
          getTimestamp(r(9).toString),
          r(10).toString.toDouble))
    icuStays
  }

  def loadLabResults(spark: SparkSession): RDD[LabResult] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, labResultsPath)
    val labResults = sqlContext.sql(
      """
        |SELECT SUBJECT_ID,
        |IFNULL(HADM_ID, '-1'),
        |ITEMID, 
        |CHARTTIME,
        |VALUENUM,
        |IFNULL(FLAG, '')
        |FROM LABEVENTS
        |WHERE (ITEMID IS NOT NULL) AND (VALUENUM IS NOT NULL)
     """.stripMargin).
      map(r =>
        LabResult(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          getTimestamp(r(3).toString.trim),
          r(4).toString.toDouble,
          r(5).toString.trim.toLowerCase == "abnormal"))
    labResults.rdd
  }

  def loadOutputEvents(spark: SparkSession): RDD[OutputEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, outputEventsPath)
    val outputEvents = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, 
        |HADM_ID,
        |IFNULL(ICUSTAY_ID, '-1'),
        |CHARTTIME,
        |ITEMID, 
        |VALUE
        |FROM OUTPUTEVENTS WHERE (HADM_ID IS NOT NULL) AND (VALUE IS NOT NULL)
      """.stripMargin).
      map(r =>
        OutputEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          r(2).toString.toLong,
          getTimestamp(r(3).toString.trim),
          r(4).toString.toLong,
          r(5).toString.toDouble))
    outputEvents.rdd
  }

  // Below are "homemade" datasets
  def loadVitals(spark: SparkSession): Dataset[Vital] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val vitalsDF = CSVHelper.loadCSVAsTable(spark, vitalsPath)
    vitalsDF.map(row =>
      Vital(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong,
        row.getString(4).toInt,
        row.getString(5).toDouble))
  }

  def loadBloodChemistry(spark: SparkSession): RDD[BloodChemistry] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val bloodChemistryDF = CSVHelper.loadCSVAsTable(spark, bloodChemistryPath)
    bloodChemistryDF.rdd.map(row =>
      BloodChemistry(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong,
        row.getString(4).toInt,
        row.getString(5).toDouble,
        row.getString(6).toBoolean))
  }

  def loadNoteEvents(spark: SparkSession): RDD[NoteEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVWithMultilineText(spark, noteEventsPath)
    val noteEvents = sqlContext.sql(
      """
        |SELECT SUBJECT_ID, 
        | IFNULL(HADM_ID, '-1'),
        | IFNULL(CHARTTIME, CHARTDATE),
        | IFNULL(TEXT, '')
        |FROM NOTEEVENTS
      """.stripMargin).
      map(r =>
        NoteEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          getDateAndHourNoteEvents(r(2).toString.trim),
          r(3).toString))
    noteEvents.rdd
  }

  def loadFirstEvents(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, firstEventsPath)
    sqlContext.sql(
      """
        |SELECT cast(patientID as long) as patientID, 
        |cast(hadmID as long) as hadmID,
        |cast(dateTime as long) as dateTime
        |FROM FIRSTEVENTS
      """.stripMargin)
  }

  def loadWeightsFromNotes(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, weightsFromNotesPath)
    sqlContext.sql(
      """
        |SELECT cast(patientID as long) as patientID, 
        |cast(hadmID as long) as hadmID,
        |cast(chartTime as long) as chartTime,
        |cast(weight as double) as weight
        |FROM PATIENTWEIGHTSFROMNOTES
      """.stripMargin)
  }

  def loadSaO2FiO2Events(spark: SparkSession): RDD[SaO2FiO2Ratio] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val saO2FiO2Events = CSVHelper.loadCSVAsTable(spark, saO2fiO2RatioPath)
    saO2FiO2Events.rdd.map(row =>
      SaO2FiO2Ratio(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toDouble,
        row.getString(4).toLong))
  }

  def loadPulsePressures(spark: SparkSession): Dataset[PulsePressure] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val pressures = CSVHelper.loadCSVAsTable(spark, pulsePressuresPath)
    pressures.map(row =>
      PulsePressure(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toDouble,
        row.getString(4).toLong))
  }

  def loadAverageWeights(spark: SparkSession): RDD[AverageWeight] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val avgWeights = CSVHelper.loadCSVAsTable(spark, avgPatientWeightsPath)
    avgWeights.rdd.map(row =>
      AverageWeight(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toDouble))
  }

  def loadVasopressors(spark: SparkSession): RDD[Vasopressor] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val vasopressors = CSVHelper.loadCSVAsTable(spark, vasopressorsPath)
    vasopressors.rdd.map(row =>
      Vasopressor(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong,
        row.getString(4).toLong,
        row.getString(5).toInt,
        row.getString(6).toDouble,
        row.getString(7).toDouble))
  }

  def loadComaScores(spark: SparkSession): Dataset[ComaScore] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val comaScores = CSVHelper.loadCSVAsTable(spark, comaScoresPath)
    comaScores.map(row =>
      ComaScore(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toDouble))
  }

  def loadUrineOutput(spark: SparkSession): RDD[UrineOutputEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val urineOutput = CSVHelper.loadCSVAsTable(spark, urineOutputPath)
    urineOutput.rdd.map(row =>
      UrineOutputEvent(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong,
        row.getString(4).toDouble))
  }

  def loadSofaLabs(spark: SparkSession): RDD[SofaLab] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val sofaLabs = CSVHelper.loadCSVAsTable(spark, sofaLabsPath)
    sofaLabs.rdd.map(row =>
      SofaLab(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong,
        row.getString(4).toDouble))
  }

  def loadSofaFeatures(spark: SparkSession): RDD[SofaFeatures] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val sofaFeatures = CSVHelper.loadCSVAsTable(spark, sofaFeaturesPath)
    sofaFeatures.rdd.map(row =>
      SofaFeatures(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toDouble,
        row.getString(4).toDouble,
        row.getString(5).toDouble,
        row.getString(6).toDouble,
        row.getString(7).toDouble,
        row.getString(8).toDouble,
        row.getString(9).toDouble,
        row.getString(10).toDouble,
        row.getString(11).toDouble,
        row.getString(12).toDouble,
        row.getString(13).toDouble))
  }

  def loadSofaScores(spark: SparkSession): RDD[SofaScore] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val sofaScores = CSVHelper.loadCSVAsTable(spark, sofaScoresPath)
    sofaScores.rdd.map(row =>
      SofaScore(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toInt,
        row.getString(4).toInt,
        row.getString(5).toInt,
        row.getString(6).toInt,
        row.getString(7).toInt,
        row.getString(8).toInt,
        row.getString(9).toInt))
  }

  def loadSofaWindows(spark: SparkSession): RDD[SofaWindow] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext
    val sofaScores = CSVHelper.loadCSVAsTable(spark, sofaWindowPath)

    sofaScores.rdd.map(row =>
      SofaWindow(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toLong,
        row.getString(3).toLong))
  }

  def loadAdmissions(spark: SparkSession): Dataset[AdmitEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, admissionsDataPath)
    sqlContext.sql(
      """
        |SELECT cast(SUBJECT_ID as long) as patientID, 
        |cast(HADM_ID as long) as hadmID,
        |ADMITTIME as admitTime
        |FROM ADMISSIONS
      """.stripMargin)
      .map(r =>
        AdmitEvent(
          r(0).toString.toLong,
          r(1).toString.toLong,
          getTimestamp(r(2).toString.trim)))
  }

  def loadLabels(spark: SparkSession): Dataset[LabelEvent] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    CSVHelper.loadCSVAsTable(spark, patientLabelsPath)
    sqlContext.sql(
      """
        |SELECT patientID, 
        |hadmID,
        |onsetDelta,
        |timeLabeledSeptic,
        |windowStart,
        |isSeptic
        |FROM PATIENTLABELS
      """.stripMargin)
      .map(r => LabelEvent(
        r(0).toString.toLong,
        r(1).toString.toLong,
        r(2).toString.toInt,
        r(3).toString.toLong,
        r(4).toString.toLong,
        r(5).toString.toInt))
  }

  def loadModelFeatures(hourMax: Int, spark: SparkSession): Dataset[ModelFeatures] = {
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val path = dataPath + "features" + hourMax.toString + ".csv"
    val features = CSVHelper.loadCSVAsTable(spark, path)
    features.map(row => {
      ModelFeatures(
        row.getString(0).toLong,
        row.getString(1).toLong,
        row.getString(2).toDouble,
        row.getString(3).toDouble,
        row.getString(4).toDouble,
        row.getString(5).toDouble,
        row.getString(6).toDouble,
        row.getString(7).toDouble,
        row.getString(8).toDouble,
        row.getString(9).toDouble,
        row.getString(10).toDouble,
        row.getString(11).toDouble,
        row.getString(12).toDouble,
        row.getString(13).toDouble,
        row.getString(14).toDouble,
        row.getString(15).toInt)
    })
  }
}
