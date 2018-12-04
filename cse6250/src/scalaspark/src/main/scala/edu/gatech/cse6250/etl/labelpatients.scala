package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._

object LabelInfo {

  def labelPatients(sofaScores: RDD[SofaScore], sofaWindows: RDD[SofaWindow]): RDD[LabelEvent] = {
    val numMiliInHour = 1000 * 60 * 60
    val keyedSofaWindows = sofaWindows.map(w => ((w.patientID, w.hadmID), w))

    sofaScores.map(s => ((s.patientID, s.hadmID), s))
      .join(keyedSofaWindows)
      .filter({ // Filter out scores that are outside of the window
        case ((pid, aid), (score, window)) => {
          score.chartTime >= window.windowStart && score.chartTime <= window.windowEnd
        }
      })
      .map({
        case ((pid, aid), (score, window)) => {
          ((pid, aid, window.windowStart), score)
        }
      })
      .groupByKey()
      .map({
        case ((pid, aid, startTime), iterableOfScores) => {
          val sortedScores = iterableOfScores.toList.sortBy(s => s.chartTime)

          var firstScore = sortedScores.head.score
          var label = 0
          var timeLabeled: Long = 0

          breakable {
            for (s <- sortedScores.drop(1)) {
              // if score has increased by at least 2 points label patient as septic
              if (s.score >= firstScore + 2) {
                label = 1
                timeLabeled = s.chartTime
                break
              }

            }
          }

          val onsetDelta = (timeLabeled - startTime) / numMiliInHour
          LabelEvent(pid, aid, onsetDelta.toInt, timeLabeled, startTime, label)
        }
      })
  }

}
