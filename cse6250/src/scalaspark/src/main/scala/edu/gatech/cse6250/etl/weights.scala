package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import org.apache.spark.rdd.RDD
import scala.util.control.NonFatal
import org.apache.spark.sql.{ Dataset, DataFrame }

object WeightInfo {

  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/echo-data.sql
  def getPatientWeightsFromNotes(noteEvents: RDD[NoteEvent]): RDD[Weight] = {
    val pattern = """Weight \(lb\): (.*)\n""".r

    noteEvents.filter(n => {
      pattern.findFirstMatchIn(n.text) match {
        case Some(i) => {
          try {
            i.group(1).toDouble
            true
          } catch {
            case NonFatal(_) => false
          }
        }
        case None => false
      }
    })
      .map(n => {
        pattern.findFirstMatchIn(n.text) match {
          case Some(i) => {
            Weight(n.patientID, n.hadmID, n.chartTime, i.group(1).toDouble)
          }
        }
      })

  }

  def combineWeightsFromEchoAndChartEvents(chartEvents: Dataset[ChartEvent], weightsFromNotes: DataFrame): DataFrame = {
    val spark = chartEvents.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val transformedWeightsFromNotes = weightsFromNotes.withColumn("weight", $"weight" * 0.45359237)

    val weightItemIds = List(762, 763, 3723, 3580, 3581, 3582, 226512)
    val transformedChartEventWeights = chartEvents.filter(chartEvents("itemID").isin(weightItemIds: _*))
      .map(c => {
        val newValue = c.itemID match {
          case 3581 => c.value * 0.45359237
          case 3582 => c.value * 0.0283495231
          case _ => c.value
        }

        c.copy(value = newValue)
      }).select($"patientID", $"hadmID", $"chartTime", $"value".alias("weight"))

    transformedChartEventWeights.union(transformedWeightsFromNotes)
      .groupBy("patientID", "hadmID")
      .agg(avg("weight").alias("avgWeight"))
  }

}