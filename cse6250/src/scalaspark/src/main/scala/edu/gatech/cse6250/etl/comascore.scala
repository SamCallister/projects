package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import scala.math.min

object ComaScoreInfo {

  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/firstday/gcs-first-day.sql
  def getComaScores(chartEvents: RDD[ChartEvent]): RDD[ComaScore] = {

    chartEvents.filter((c: ChartEvent) => {
      c.itemID match {
        case 184 => true
        case 454 => true
        case 723 => true
        case 223900 => true
        case 223901 => true
        case 220739 => true
        case _ => false
      }
    })
      .map((c: ChartEvent) => {
        val (id: Int, value: Double) = c.itemID match {
          case 732 => if (c.valueString == "1.0 ET/Trach") (1, 0.0) else (1, c.value) //Verbal
          case 223900 => if (c.valueString == "No Response-ETT") (1, 0.0) else (1, c.value) //Verbal
          case 454 => (2, c.value) // Motor
          case 223901 => (2, c.value) // Motor
          case _ => (3, c.value) // Eyes
        }

        ((c.patientID, c.hadmID), (c.chartTime, id, value))
      })
      .groupByKey()
      .flatMap {
        case ((pid: Long, hid: Long), i: Iterable[(Long, Int, Double)]) => {

          // Default comma score values when everything is fine
          var verbal = 5.0
          var motor = 6.0
          var eyes = 4.0

          // Move through time updating the glascow coma score special case reset prev
          // values if was previously intubated and then wasn't
          i.toList.sortWith(_._1 < _._1)
            .map {
              case (chartTime: Long, id: Int, value: Double) => {
                id match {
                  case 1 => { // Verbal
                    // If they were intubated but are not any more do not use previous GCS values
                    if (verbal == 0 && value != 0) {
                      // Reset prev values
                      motor = 6
                      eyes = 4
                    }
                    verbal = value
                  }
                  case 2 => motor = value // Motor
                  case 3 => eyes = value // Eyes
                }
                val score = verbal + motor + eyes

                ((pid, hid, chartTime), score)
              }
            }
        }
      }
      .reduceByKey(min)
      .map {
        case ((pid, hid, chartTime), score) => ComaScore(pid, hid, chartTime, score)
      }
  }
}