package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.helper.SparkHelper
import edu.gatech.cse6250.utils.DateUtils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SofaFeaturesInfo {
  val CREATININE = 50912
  val BILIRUBIN = 50885
  val PLATELET = 51265

  class AvgCollector {
    var tot: Double = 0
    var count: Int = 0

    def this(tot: Option[Double], count: Int = 0) = {
      this()
      tot match {
        case Some(t) => {
          this.tot = t
          this.count = 1
        }
        case None => {
          this.tot = 0
          this.count = 0
        }
      }
    }
    def combine(that: AvgCollector): AvgCollector = {
      new AvgCollector(Some(tot + that.tot), count + that.count)
    }
    def avg: Double = {
      if (count > 0) tot / count else -1.0
    }
  }

  case class LabSet(creatinine: AvgCollector, bilirubin: AvgCollector, platelets: AvgCollector)
  case class VPSet(epinephrine: AvgCollector, norepinephrine: AvgCollector,
    dopamine: AvgCollector, dobutamine: AvgCollector)

  def expandLab(itemID: Long, value: Double): LabSet = {
    // For a single lab event, report three fields: Creatinine, Bilirubin, Platelets.
    // This way we can do only one reduceByKey and consume all three values at once.
    // Hopefully!
    itemID match {
      case CREATININE => LabSet(new AvgCollector(Some(value)), new AvgCollector(None), new AvgCollector(None))
      case BILIRUBIN => LabSet(new AvgCollector(None), new AvgCollector(Some(value)), new AvgCollector(None))
      case _ => LabSet(new AvgCollector(None), new AvgCollector(None), new AvgCollector(Some(value)))
    }
  }

  def expandVasopressors(vpID: Int, rate: Double): VPSet = {
    // Similar to above, report all fields for a single row.
    // Use default for values not provided.
    Vasopressor(vpID) match {
      case Vasopressor.Epinephrine => VPSet(
        new AvgCollector(Some(rate)),
        new AvgCollector(None), new AvgCollector(None), new AvgCollector(None))
      case Vasopressor.Norepinephrine => VPSet(
        new AvgCollector(None),
        new AvgCollector(Some(rate)),
        new AvgCollector(None), new AvgCollector(None))
      case Vasopressor.Dopamine => VPSet(
        new AvgCollector(None), new AvgCollector(None),
        new AvgCollector(Some(rate)),
        new AvgCollector(None))
      case Vasopressor.Dobutamine => VPSet(
        new AvgCollector(None), new AvgCollector(None), new AvgCollector(None),
        new AvgCollector(Some(rate)))
    }
  }

  def getAggregatedFeatures(vitals: RDD[Vital], labs: RDD[SofaLab], urineOutput: RDD[UrineOutputEvent],
    comaScores: RDD[ComaScore], saO2FiO2Ratios: RDD[SaO2FiO2Ratio],
    vasopressors: RDD[Vasopressor]): RDD[SofaFeatures] = {

    // Step 1: get all the components by hour

    def reduceLabValues(l1: LabSet, l2: LabSet): LabSet = {
      LabSet(
        l1.creatinine combine l2.creatinine,
        l1.bilirubin combine l2.bilirubin,
        l1.platelets combine l2.platelets)
    }

    val hourlyLabs = labs.
      map(l => ((l.patientID, l.hadmID, quantizeToHour(l.chartTime)), expandLab(l.itemID, l.value))).
      reduceByKey(reduceLabValues)

    def reduceVasoRates(v1: VPSet, v2: VPSet): VPSet = {
      VPSet(
        v1.epinephrine combine v2.epinephrine,
        v1.norepinephrine combine v2.norepinephrine,
        v1.dopamine combine v2.dopamine,
        v1.dobutamine combine v2.dobutamine)
    }

    val hourlyVaso = vasopressors.
      map(v => ((v.patientID, v.hadmID, quantizeToHour(v.startTime)), expandVasopressors(v.vpID, v.rate))).
      reduceByKey(reduceVasoRates)

    val minHourlyComaScores = comaScores.
      map(c => ((c.patientID, c.hadmID, quantizeToHour(c.chartTime)), new AvgCollector(Some(c.score)))).
      reduceByKey(_ combine _)

    val netDailyUrineOutput = urineOutput.
      map(u => ((u.patientID, u.hadmID, quantizeToDay(u.chartTime)), u.amount)).
      reduceByKey(_ + _).
      map { case ((pid, haid, day), amount) => ((pid, haid), (day, amount)) }

    val hourlyUrineOuput = urineOutput.
      map(u => ((u.patientID, u.hadmID), quantizeToHour(u.chartTime))).
      distinct.
      join(netDailyUrineOutput).
      filter {
        case ((pid, haid), (hour, (day, amount))) => {
          quantizeToDay(hour) == day
        }
      }.map {
        case ((pid, haid), (hour, (_, amount))) => {
          ((pid, haid, hour), amount)
        }
      }

    val minHourlySaO2FiO2Ratios = saO2FiO2Ratios.
      map(r => ((r.patientID, r.hadmID, quantizeToHour(r.chartTime)), new AvgCollector(Some(r.ratio)))).
      reduceByKey(_ combine _)

    val minHourlyMAP = vitals.
      filter(_.vitalID == Vital.MeanBP.id).
      map(v => ((v.patientID, v.hadmID, quantizeToHour(v.chartTime)), new AvgCollector(Some(v.value)))).
      reduceByKey(_ combine _)

    // Step 2: get all distinct data points
    // Add a dummy value in order to perform a left outer join.

    val allPoints = hourlyLabs.keys.
      union(hourlyVaso.keys).
      union(minHourlyComaScores.keys).
      union(hourlyUrineOuput.keys).
      union(minHourlySaO2FiO2Ratios.keys).
      union(minHourlyMAP.keys).
      distinct.
      map { case (pid, haid, ktime) => ((pid, haid, ktime), 0) }

    // Step 3: do a left outer join on all data

    val combined = allPoints.
      leftOuterJoin(hourlyLabs).
      leftOuterJoin(hourlyVaso).
      leftOuterJoin(minHourlyComaScores).
      leftOuterJoin(hourlyUrineOuput).
      leftOuterJoin(minHourlySaO2FiO2Ratios).
      leftOuterJoin(minHourlyMAP)

    // Step 4: map all values to SofaFeatures, using -1.0 for missing values
    // Impute backwards.
    impute(
      combined.map {
        case ((pid, haid, stime),
          ((((((_, oLabSet), oVPSet), oMinGCS), oNetUrine), oRespRatio), oMinMAP)) => {
          val (maxCreatinine, maxBilirubin, minPlatelets) = oLabSet match {
            case Some(ls) => (ls.creatinine.avg, ls.bilirubin.avg, ls.platelets.avg)
            case None => (-1.0, -1.0, -1.0)
          }
          val (maxEpinephrine, maxNorepinephrine, maxDopamine, maxDobutamine) = oVPSet match {
            case Some(vs) => (vs.epinephrine.avg, vs.norepinephrine.avg, vs.dopamine.avg, vs.dobutamine.avg)
            case None => (-1.0, -1.0, -1.0, -1.0)
          }
          val minGCS = oMinGCS match {
            case Some(s) => s.avg
            case None => -1
          }
          val netUrine = oNetUrine match {
            case Some(n) => n
            case None => -1
          }
          val minRespRatio = oRespRatio match {
            case Some(r) => r.avg
            case None => -1
          }
          val minMAP = oMinMAP match {
            case Some(m) => m.avg
            case None => -1
          }
          SofaFeatures(pid, haid, stime, minRespRatio,
            minPlatelets, maxBilirubin, minMAP,
            maxNorepinephrine, maxEpinephrine, maxDopamine, maxDobutamine,
            minGCS, maxCreatinine, netUrine)
        }
      })
  }

  case class Imputable(patientID: Long, chartTime: Long, value: Double, sofaFeatures: SofaFeatures)
  case class ImputablePair(missing: Imputable, nonMissing: Imputable)

  def minImputablePairByDate(i1: ImputablePair, i2: ImputablePair): ImputablePair = {
    if (i1.nonMissing.chartTime < i2.nonMissing.chartTime) i1 else i2
  }

  def imputeFeature(allValues: RDD[Imputable]): RDD[Imputable] = {
    val nonMissing = allValues.filter(_.value >= 0).map(v => (v.patientID, v))
    val missing = allValues.filter(_.value < 0).map(v => (v.patientID, v))
    val imputed = missing.join(nonMissing).
      filter { case (pid, (m, nm)) => m.chartTime < nm.chartTime }.
      map { case (pid, (m, nm)) => ((pid, m.chartTime), ImputablePair(m, nm)) }.
      reduceByKey(minImputablePairByDate).
      map {
        case ((pid, mTime), iPair) =>
          ((pid, mTime), Imputable(pid, mTime, iPair.nonMissing.value, iPair.missing.sofaFeatures))
      }
    allValues.map(i => ((i.patientID, i.chartTime), i)).
      leftOuterJoin(imputed).
      map {
        case ((pid, cTime), (v, oImputed)) => {
          oImputed match {
            case Some(iv) => iv
            case None => v
          }
        }
      }
  }

  def imputeRespRatios(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.minRespRatio, s))).
      map(i => i.sofaFeatures.withMinRespRatio(i.value))
  }

  def imputePlatelets(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.minPlatelets, s))).
      map(i => i.sofaFeatures.withMinPlatelets(i.value))
  }

  def imputeBilirubin(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxBilirubin, s))).
      map(i => i.sofaFeatures.withMaxBilirubin(i.value))
  }

  def imputeMAP(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.minMeanBP, s))).
      map(i => i.sofaFeatures.withMinMeanBP(i.value))
  }

  def imputeNorepinephrine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxNorepinephrine, s))).
      map(i => i.sofaFeatures.withMaxNorepinephrine(i.value))
  }

  def imputeEpinephrine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxEpinephrine, s))).
      map(i => i.sofaFeatures.withMaxEpinephrine(i.value))
  }

  def imputeDopamine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxDopamine, s)))
      .map(i => i.sofaFeatures.withMaxDopamine(i.value))
  }

  def imputeDobutamine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxDobutamine, s))).
      map(i => i.sofaFeatures.withMaxDobutamine(i.value))
  }

  def imputeGCS(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.minGCS, s))).
      map(i => i.sofaFeatures.withMinGCS(i.value))
  }

  def imputeCreatinine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.maxCreatinine, s))).
      map(i => i.sofaFeatures.withMaxCreatinine(i.value))
  }

  def imputeUrine(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    imputeFeature(sofaFeatures.
      map(s => Imputable(s.patientID, s.chartTime, s.netUrine, s))).
      map(i => i.sofaFeatures.withNetUrine(i.value))
  }

  def impute(sofaFeatures: RDD[SofaFeatures]): RDD[SofaFeatures] = {
    val withResp = imputeRespRatios(sofaFeatures)
    val withPlatelets = imputePlatelets(withResp)
    val withBilirubin = imputeBilirubin(withPlatelets)
    val withMinMeanBP = imputeMAP(withBilirubin)
    val withNorepinephrine = imputeNorepinephrine(withMinMeanBP)
    val withEpinephrine = imputeEpinephrine(withNorepinephrine)
    val withDopamine = imputeDopamine(withEpinephrine)
    val withDobutamine = imputeDobutamine(withDopamine)
    val withGCS = imputeGCS(withDobutamine)
    val withCreatinine = imputeCreatinine(withGCS)
    imputeUrine(withCreatinine)
  }
}
