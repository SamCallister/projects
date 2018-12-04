package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SparkSession, Dataset }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.math.{ min => dmin }
import scala.math.{ max => dmax }

object NormalValues {
  val systolicBP: Double = 120
  val pulsePressure: Double = 50
  val heartRate: Double = 80
  val respRate: Double = 16
  val temp: Double = 37
  val spO2: Double = 96.5
  val gCS: Double = 15
}

object FeatureInfo {

  def getFeatures(vitals: Dataset[Vital], cs: Dataset[ComaScore], pp: Dataset[PulsePressure], labels: Dataset[LabelEvent]): RDD[Features] = {
    val spark = vitals.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val numMiliInHour: Long = 1000 * 60 * 60

    val neededVitals = List(0, 1, 4, 5, 6)
    val vitalsByHour = vitals.filter($"vitalID".isin(neededVitals: _*))
      .withColumn("chartTime", $"chartTime" - ($"chartTime" % numMiliInHour))
    val pivotedOnVitalType = vitalsByHour.groupBy("patientID", "hadmID", "chartTime").pivot("vitalID")
    val minVitals = pivotedOnVitalType.min("value")
      .select($"patientID", $"hadmID", $"chartTime", $"0".alias("minHeartRate"), $"1".alias("minSystolicBP"), $"4".alias("minRespRate"), $"5".alias("minTempC"), $"6".alias("minSpO2"))
    val maxVitals = pivotedOnVitalType.max("value")
      .select($"patientID", $"hadmID", $"chartTime", $"0".alias("maxHeartRate"), $"1".alias("maxSystolicBP"), $"4".alias("maxRespRate"), $"5".alias("maxTempC"), $"6".alias("maxSpO2"))
    val vitalFeatures = minVitals.join(maxVitals, Seq("patientID", "hadmID", "chartTime"))

    val ppByHour = pp.withColumn("chartTime", $"chartTime" - ($"chartTime" % numMiliInHour))
      .groupBy("patientID", "hadmID", "chartTime")
    val minPulseValues = ppByHour.agg(min("value").alias("minPulsePressure"))
    val maxPulseValues = ppByHour.agg(max("value").alias("maxPulsePressure"))
    val pulseFeatures = minPulseValues.join(maxPulseValues, Seq("patientID", "hadmID", "chartTime"))

    val features = vitalFeatures.join(pulseFeatures, Seq("patientID", "hadmID", "chartTime"), "outer")
      .join(cs, Seq("patientID", "hadmID", "chartTime"), "outer")

    val labelInfo = labels.withColumn("fourHoursBefore", $"timeLabeledSeptic" - (numMiliInHour * 4))

    val featuresAfterWindowBegins = features.join(labelInfo, Seq("patientID", "hadmID"))
      .filter($"chartTime" >= $"windowStart")

    val featuresWithoutSepsis = featuresAfterWindowBegins.filter($"isSeptic" === 0)

    val allFeatures = featuresAfterWindowBegins.filter($"isSeptic" === 1)
      .filter($"chartTime" < $"fourHoursBefore")
      .union(featuresWithoutSepsis)
      .withColumn("hourDelta", (($"chartTime" - $"windowStart") / numMiliInHour).cast("int"))
      .select($"patientID", $"hadmID", $"chartTime", $"minSystolicBP", $"maxSystolicBP", $"minPulsePressure", $"maxPulsePressure", $"maxHeartRate", $"minHeartRate", $"minRespRate", $"maxRespRate", $"minTempC", $"maxTempC", $"minSpO2", $"score".alias("minGCS"), $"hourDelta", $"isSeptic")
      .as[OptionFeatures]

    val res = allFeatures.rdd.map(f => ((f.patientID, f.hadmID), f))
      .groupByKey()
      .flatMap({
        case ((pid, hid), featuresIterable) => {
          val sortedFeatures = featuresIterable.toList.sortBy(f => f.hourDelta)
          var seenList: ListBuffer[OptionFeatures] = ListBuffer(sortedFeatures.head)
          var resultList: ListBuffer[Features] = ListBuffer(fillNullsWithDefaults(sortedFeatures.head))

          for (f <- sortedFeatures.drop(1)) {
            seenList += f
            val newResult = Features(
              f.patientID,
              f.hadmID,
              f.chartTime,
              reduceOrDefault(seenList.map(_.minSystolicBP), dmin, NormalValues.systolicBP),
              reduceOrDefault(seenList.map(_.maxSystolicBP), dmax, NormalValues.systolicBP),
              reduceOrDefault(seenList.map(_.minPulsePressure), dmin, NormalValues.pulsePressure),
              reduceOrDefault(seenList.map(_.maxPulsePressure), dmax, NormalValues.pulsePressure),
              reduceOrDefault(seenList.map(_.minHeartRate), dmin, NormalValues.heartRate),
              reduceOrDefault(seenList.map(_.maxHeartRate), dmax, NormalValues.heartRate),
              reduceOrDefault(seenList.map(_.minRespRate), dmin, NormalValues.respRate),
              reduceOrDefault(seenList.map(_.maxRespRate), dmax, NormalValues.respRate),
              reduceOrDefault(seenList.map(_.minTempC), dmin, NormalValues.temp),
              reduceOrDefault(seenList.map(_.maxTempC), dmax, NormalValues.temp),
              reduceOrDefault(seenList.map(_.minSpO2), dmin, NormalValues.spO2),
              reduceOrDefault(seenList.map(_.minGCS), dmin, NormalValues.gCS),
              f.hourDelta,
              f.isSeptic)
            resultList += newResult
          }

          resultList
        }
      })
    res
  }

  def reduceOrDefault(values: ListBuffer[Option[Double]], f: (Double, Double) => Double, default: Double): Double = {
    val nonNull = values.filter(_.isDefined).map(_.get)
    if (nonNull.isEmpty) {
      default
    } else {
      nonNull.reduce(f)
    }
  }

  def fillNullsWithDefaults(value: OptionFeatures): Features = {
    Features(
      value.patientID,
      value.hadmID,
      value.chartTime,
      value.minSystolicBP.getOrElse(NormalValues.systolicBP),
      value.maxSystolicBP.getOrElse(NormalValues.systolicBP),
      value.minPulsePressure.getOrElse(NormalValues.pulsePressure),
      value.maxPulsePressure.getOrElse(NormalValues.pulsePressure),
      value.minHeartRate.getOrElse(NormalValues.heartRate),
      value.maxHeartRate.getOrElse(NormalValues.heartRate),
      value.minRespRate.getOrElse(NormalValues.respRate),
      value.maxRespRate.getOrElse(NormalValues.respRate),
      value.minTempC.getOrElse(NormalValues.temp),
      value.maxTempC.getOrElse(NormalValues.temp),
      value.minSpO2.getOrElse(NormalValues.spO2),
      value.minGCS.getOrElse(NormalValues.gCS),
      value.hourDelta,
      value.isSeptic)
  }

}