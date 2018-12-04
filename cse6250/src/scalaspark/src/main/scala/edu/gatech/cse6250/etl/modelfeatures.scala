package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.helper.SparkHelper
import edu.gatech.cse6250.utils.DateUtils._
import edu.gatech.cse6250.utils.Collectors.MinMaxCollector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import scala.collection.mutable.ArrayBuffer

object ModelFeaturesInfo {

  case class AggregatedVitals(
    MinMaxSysBP: MinMaxCollector,
    MinMaxHeartRate: MinMaxCollector,
    MinMaxRespRate: MinMaxCollector,
    MinMaxTempC: MinMaxCollector,
    MinMaxSpO2: MinMaxCollector) {

    def combine(that: AggregatedVitals): AggregatedVitals = {
      AggregatedVitals(
        this.MinMaxSysBP combine that.MinMaxSysBP,
        this.MinMaxHeartRate combine that.MinMaxHeartRate,
        this.MinMaxRespRate combine that.MinMaxRespRate,
        this.MinMaxTempC combine that.MinMaxTempC,
        this.MinMaxSpO2 combine that.MinMaxSpO2)
    }
  }

  def aggregateVitals(
    windowMax: Long,
    vitals: RDD[Vital],
    keyedWindows: RDD[((Long, Long), Long)]): RDD[((Long, Long), AggregatedVitals)] = {

    val REQUIRED = Set(Vital.SysBP.id, Vital.HeartRate.id, Vital.RespRate.id, Vital.TempC.id, Vital.SpO2.id)

    vitals.map(v => ((v.patientID, v.hadmID), v)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (v, windowStart)) =>
          REQUIRED(v.vitalID) &&
            (v.chartTime >= windowStart) && (v.chartTime <= (windowStart + windowMax))
      }.
      map {
        case ((pid, haid), (v, windowStart)) => {
          val mmc = if (v.value < 0)
            MinMaxCollector.empty(windowStart + windowMax)
          else
            MinMaxCollector.withValue(v.value, windowStart + windowMax)
          val aggVitals = Vital(v.vitalID) match {
            case Vital.SysBP => AggregatedVitals(
              mmc,
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime))
            case Vital.HeartRate => AggregatedVitals(
              MinMaxCollector.empty(mmc.lastChartTime),
              mmc,
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime))
            case Vital.RespRate => AggregatedVitals(
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              mmc,
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime))
            case Vital.TempC => AggregatedVitals(
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              mmc,
              MinMaxCollector.empty(mmc.lastChartTime))
            case Vital.SpO2 => AggregatedVitals(
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              MinMaxCollector.empty(mmc.lastChartTime),
              mmc)
          }
          ((pid, haid), aggVitals)
        }
      }.reduceByKey(_ combine _)
  }

  def getAllPoints(windowMax: Long, vitals: RDD[Vital], comaScores: RDD[ComaScore], pulsePressures: RDD[PulsePressure],
    keyedWindows: RDD[((Long, Long), Long)]): RDD[((Long, Long), Long)] = {
    val vSubset = vitals.map(v => ((v.patientID, v.hadmID), v)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (v, windowStart)) =>
          (v.chartTime >= windowStart) && (v.chartTime <= (windowStart + windowMax))
      }.
      map { case ((pid, haid), (v, _)) => ((pid, haid), v) }

    val csSubset = comaScores.map(c => ((c.patientID, c.hadmID), c)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (c, windowStart)) =>
          (c.chartTime >= windowStart) && (c.chartTime <= (windowStart + windowMax))
      }.
      map { case ((pid, haid), (c, _)) => ((pid, haid), c) }

    val pSubset = pulsePressures.map(p => ((p.patientID, p.hadmID), p)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (p, windowStart)) =>
          (p.chartTime >= windowStart) && (p.chartTime <= (windowStart + windowMax))
      }.
      map { case ((pid, haid), (p, _)) => ((pid, haid), p) }

    vSubset.keys.
      union(pSubset.keys).
      union(csSubset.keys).
      distinct.
      map { case (pid, haid) => ((pid, haid), 0) }

  }

  def aggregatePulsePressures(windowMax: Long, pulsePressures: RDD[PulsePressure],
    keyedWindows: RDD[((Long, Long), Long)]): RDD[((Long, Long), MinMaxCollector)] = {

    val pSubset = pulsePressures.map(p => ((p.patientID, p.hadmID), p)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (p, windowStart)) =>
          (p.chartTime >= windowStart) && (p.chartTime <= (windowStart + windowMax))
      }

    pSubset.map {
      case ((pid, haid), (p, windowStart)) => {
        val mmVal = if (p.value < 0) None else Some(p.value)
        ((pid, haid), new MinMaxCollector(mmVal, windowStart + windowMax))
      }
    }.reduceByKey(_ combine _)
  }

  def aggregateComaScores(windowMax: Long, comaScores: RDD[ComaScore],
    keyedWindows: RDD[((Long, Long), Long)]): RDD[((Long, Long), MinMaxCollector)] = {

    val csSubset = comaScores.map(c => ((c.patientID, c.hadmID), c)).
      join(keyedWindows).
      filter {
        case ((pid, haid), (c, windowStart)) =>
          (c.chartTime >= windowStart) && (c.chartTime <= (windowStart + windowMax))
      }

    csSubset.map {
      case ((pid, haid), (c, windowStart)) => {
        val mmVal = if (c.score < 0) None else Some(c.score)
        ((pid, haid), new MinMaxCollector(mmVal, windowStart + windowMax))
      }
    }.reduceByKey(_ combine _)
  }

  def aggregateFeatures(maxHours: Int, vitals: RDD[Vital], comaScores: RDD[ComaScore],
    pulsePressures: RDD[PulsePressure], sofaWindows: RDD[SofaWindow], labels: RDD[LabelEvent]): RDD[ModelFeatures] = {
    val keyedWindows = sofaWindows.map(w => ((w.patientID, w.hadmID), w.windowStart))
    val keyedLabels = labels.map(l => ((l.patientID, l.hadmID), l))
    val windowMax = (maxHours * HOURMILLIS).toLong

    val aggVitals = aggregateVitals(windowMax, vitals, keyedWindows)
    val aggPressures = aggregatePulsePressures(windowMax, pulsePressures, keyedWindows)
    val aggComaScores = aggregateComaScores(windowMax, comaScores, keyedWindows)
    val allPoints = getAllPoints(windowMax, vitals, comaScores, pulsePressures, keyedWindows)

    val combined = allPoints.
      leftOuterJoin(aggVitals).
      leftOuterJoin(aggPressures).
      leftOuterJoin(aggComaScores)

    val allFeatures = combined.map {
      case ((pid, haid),
        (((_, oVitals), oPressure), oComaScore)) => {
        var lastChartTime = 0L
        var minSysBP = -1.0
        var maxSysBP = -1.0
        var minHeartRate = -1.0
        var maxHeartRate = -1.0
        var minRespRate = -1.0
        var maxRespRate = -1.0
        var minTempC = -1.0
        var maxTempC = -1.0
        var minSpO2 = -1.0
        var minPressure = -1.0
        var maxPressure = -1.0
        var minCS = -1.0
        oVitals match {
          case Some(aggVital) => {
            lastChartTime = Math.max(lastChartTime, aggVital.MinMaxSysBP.lastChartTime)
            minSysBP = aggVital.MinMaxSysBP.minVal.getOrElse(-1.0)
            maxSysBP = aggVital.MinMaxSysBP.maxVal.getOrElse(-1.0)
            minHeartRate = aggVital.MinMaxHeartRate.minVal.getOrElse(-1.0)
            maxHeartRate = aggVital.MinMaxHeartRate.maxVal.getOrElse(-1.0)
            minRespRate = aggVital.MinMaxRespRate.minVal.getOrElse(-1.0)
            maxRespRate = aggVital.MinMaxRespRate.maxVal.getOrElse(-1.0)
            minTempC = aggVital.MinMaxTempC.minVal.getOrElse(-1.0)
            maxTempC = aggVital.MinMaxTempC.maxVal.getOrElse(-1.0)
            minSpO2 = aggVital.MinMaxSpO2.minVal.getOrElse(-1.0)
          }
          case _ => {}
        }
        oPressure match {
          case Some(mmc) => {
            lastChartTime = Math.max(mmc.lastChartTime, lastChartTime)
            minPressure = mmc.minVal.getOrElse(-1.0)
            maxPressure = mmc.maxVal.getOrElse(-1.0)
          }
          case _ => {}
        }
        oComaScore match {
          case Some(mmc) => {
            lastChartTime = Math.max(mmc.lastChartTime, lastChartTime)
            minCS = mmc.minVal.getOrElse(-1.0).toDouble
          }
          case _ => {}
        }
        ((pid, haid),
          ((minSysBP, maxSysBP, minHeartRate, maxHeartRate, minRespRate, maxRespRate,
            minTempC, maxTempC, minSpO2, minPressure, maxPressure, minCS), lastChartTime))
      }
    }

    allFeatures.join(keyedLabels).
      map {
        case ((pid, haid),
          (((minSysBP, maxSysBP, minHeartRate, maxHeartRate, minRespRate, maxRespRate,
            minTempC, maxTempC, minSpO2, minPressure, maxPressure, minCS), lastChartTime), l)) =>
          val label = if (l.timeLabeledSeptic <= lastChartTime) l.isSeptic else 0
          ModelFeatures(pid, haid,
            minSysBP, maxSysBP, minHeartRate, maxHeartRate, minRespRate, maxRespRate, minTempC, maxTempC,
            minSpO2, minPressure, maxPressure, minCS, 0.0, label)
      }
  }
}
