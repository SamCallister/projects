package edu.gatech.cse6250.etl
import edu.gatech.cse6250.model._
import edu.gatech.cse6250.phenotyping.SofaScorer
import edu.gatech.cse6250.helper.SparkHelper
import edu.gatech.cse6250.utils.DateUtils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SofaScoreInfo {
  def getSofaScores(sofaFeatures: RDD[SofaFeatures]): RDD[SofaScore] = {
    sofaFeatures.map(s =>
      SofaScore(
        s.patientID,
        s.hadmID,
        s.chartTime,
        SofaScorer.combinedScore(s),
        SofaScorer.respirationScore(s),
        SofaScorer.renalScore(s),
        SofaScorer.neurologicScore(s),
        SofaScorer.cardiovascularScore(s),
        SofaScorer.liverFunctionScore(s),
        SofaScorer.coagulationScore(s)))
  }
}
