package edu.gatech.cse6250.etl

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.helper.SparkHelper
import edu.gatech.cse6250.utils.DateUtils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object VasopressorInfo {
  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/severityscores/sofa.sql
  // Adjust by weight as appropriate -- see above source.
  val EPINEPHRINE = Set(30044L, 30119L, 30309L, 221289L)
  val NOREPINEPHRINE = Set(30047L, 30120L, 221906L)
  val DOPAMINE = Set(30043L, 30307L, 221662L)
  val DOBUTAMINE = Set(30042L, 30306L, 221653L)
  val NEEDSWEIGHT = Set(30047L, 30044L)
  val VASOPRESSOR = EPINEPHRINE.union(NOREPINEPHRINE).union(DOPAMINE).union(DOBUTAMINE)

  def getVasopressorID(itemID: Long): Int = {
    if (EPINEPHRINE(itemID)) {
      Vasopressor.Epinephrine.id
    } else if (NOREPINEPHRINE(itemID)) {
      Vasopressor.Norepinephrine.id
    } else if (DOPAMINE(itemID)) {
      Vasopressor.Dopamine.id
    } else if (DOBUTAMINE(itemID)) {
      Vasopressor.Dobutamine.id
    } else {
      -1
    }
  }

  def getVasopressors(inputEventsCv: RDD[InputEvent], inputEventsMv: RDD[InputEvent],
    avgWeights: RDD[AverageWeight]): RDD[Vasopressor] = {
    val weightPairs = avgWeights.map(w => ((w.patientID, w.hadmID), w))
    val vasoCv = inputEventsCv.filter(ie => VASOPRESSOR(ie.itemID)).
      map(ie => ((ie.patientID, ie.hadmID), ie)).
      join(weightPairs).
      map {
        case ((pid, haid), (v, w)) => {
          val rate = {
            if (NEEDSWEIGHT(v.itemID) && v.rate > 0) {
              v.rate / w.avgWeight
            } else {
              v.rate
            }
          }
          val vpID = getVasopressorID(v.itemID)
          Vasopressor(pid, haid, v.icuStayID, v.startTime, v.endTime, vpID, v.amount, rate)
        }
      }

    val vasoMv = inputEventsMv.filter(ie => VASOPRESSOR(ie.itemID)).
      map(ie => {
        val vpID = getVasopressorID(ie.itemID)
        Vasopressor(ie.patientID, ie.hadmID, ie.icuStayID, ie.startTime, ie.endTime, vpID, ie.amount, ie.rate)
      })

    vasoCv.union(vasoMv)
  }
}
