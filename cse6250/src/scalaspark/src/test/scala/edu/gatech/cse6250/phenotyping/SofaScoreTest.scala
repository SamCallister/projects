package edu.gatech.cse6250.phenotyping

import edu.gatech.cse6250.model._
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SofaScoreTest extends FlatSpec with BeforeAndAfter with Matchers {
  val patientID = 0L
  val hadmID = 0L
  val icuStayID = 0L
  val chartTime = 0L

  case class TestPair[T](input: T, expected: Int)
  case class RenalInfo(maxCreatinine: Double, netUrine: Double)
  case class CardioInfo(maxEpinephrine: Double, maxNorepinephrine: Double,
    maxDopamine: Double, maxDobutamine: Double, minMeanBP: Double)

  "respirationScore" should "give expected results" in {
    val minPlatelets, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine, maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine = 0.0
    val pairs: List[TestPair[Double]] = List(
      TestPair(-1.0, 0),
      TestPair(0.0, 4),
      TestPair(30.0, 4),
      TestPair(66.9, 4),
      TestPair(67.0, 3),
      TestPair(100.0, 3),
      TestPair(141.9, 3),
      TestPair(142.0, 2),
      TestPair(200.0, 2),
      TestPair(220.9, 2),
      TestPair(221.0, 1),
      TestPair(280.0, 1),
      TestPair(300.9, 1),
      TestPair(301.0, 0))
    pairs.foreach(p => {
      val sf = SofaFeatures(patientID, hadmID, icuStayID, chartTime,
        p.input, minPlatelets, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine,
        maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine)
      val actual = SofaScore.respirationScore(sf)
      println(s"$actual for $p")
      actual should be(p.expected)
    })
  }

  "renalScore" should "give expected results" in {
    val minResp, minPlatelets, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine, maxDopamine, maxDobutamine, minGCS = 0.0
    val pairs: List[TestPair[RenalInfo]] = List(
      TestPair(RenalInfo(1.2, 100.0), 4),
      TestPair(RenalInfo(1.2, 199.9), 4),
      TestPair(RenalInfo(5.1, 250.0), 4),
      TestPair(RenalInfo(1.2, 301.0), 3),
      TestPair(RenalInfo(1.2, 499.9), 3),
      TestPair(RenalInfo(3.5, 500.0), 3),
      TestPair(RenalInfo(4.9, 500.0), 3),
      TestPair(RenalInfo(3.5, 500.0), 3),
      TestPair(RenalInfo(2.0, 500.0), 2),
      TestPair(RenalInfo(3.4, 500.0), 2),
      TestPair(RenalInfo(1.2, 500.0), 1),
      TestPair(RenalInfo(1.9, 500.0), 1),
      TestPair(RenalInfo(-1.0, -1.0), 0),
      TestPair(RenalInfo(1.1, 500.0), 0))
    pairs.foreach(p => {
      val sf = SofaFeatures(patientID, hadmID, icuStayID, chartTime,
        minResp, minPlatelets, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine,
        maxDopamine, maxDobutamine, minGCS, p.input.maxCreatinine, p.input.netUrine)
      val actual = SofaScore.renalScore(sf)
      println(s"$actual for $p")
      actual should be(p.expected)
    })
  }

  "cardiovascularScore" should "give expected results" in {
    val minResp, minPlatelets, maxBilirubin, minGCS, maxCreatinine, netUrine = 0.0
    val pairs: List[TestPair[CardioInfo]] = List(
      TestPair(CardioInfo(0.2, 0.1, 0.0, 0.0, 70.0), 4),
      TestPair(CardioInfo(0.2, 0.1, 14.9, 0.0, 70.0), 4),
      TestPair(CardioInfo(0.1, 0.2, 0.0, 0.0, 70.0), 4),
      TestPair(CardioInfo(0.1, 0.2, 5.1, 0.0, 70.0), 4),
      TestPair(CardioInfo(0.09, 0.09, 15.1, 0.0, 70.0), 4),
      TestPair(CardioInfo(0.09, 0.0, 0.0, 0.0, 70.0), 3),
      TestPair(CardioInfo(0.0, 0.09, 0.0, 0.0, 70.0), 3),
      TestPair(CardioInfo(0.0, 0.09, 15.0, 0.0, 70.0), 3),
      TestPair(CardioInfo(0.0, 0.0, 5.1, 0.0, 70.0), 3),
      TestPair(CardioInfo(0.0, 0.0, 5.0, 0.0, 70.0), 2),
      TestPair(CardioInfo(0.0, 0.0, 0.1, 0.0, 70.0), 2),
      TestPair(CardioInfo(0.0, 0.0, 0.0, 0.1, 70.0), 2),
      TestPair(CardioInfo(0.0, 0.0, 0.0, 0.0, 69.1), 1),
      TestPair(CardioInfo(0.0, 0.0, 0.0, 0.0, 0.0), 0),
      TestPair(CardioInfo(-1.0, -1.0, -1.0, -1.0, -1.0), 0))
    pairs.foreach(p => {
      val sf = SofaFeatures(patientID, hadmID, icuStayID, chartTime,
        minResp, minPlatelets, maxBilirubin, p.input.minMeanBP, p.input.maxNorepinephrine,
        p.input.maxEpinephrine, p.input.maxDopamine, p.input.maxDobutamine, minGCS,
        maxCreatinine, netUrine)
      val actual = SofaScore.cardiovascularScore(sf)
      println(s"$actual for $p")
      actual should be(p.expected)
    })
  }

  "liverFunctionScore" should "give expected results" in {
    val minResp, minPlatelets, minMeanBP, maxNorephinephrine, maxEpinephrine, maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine = 0.0
    val pairs: List[TestPair[Double]] = List(
      TestPair(-1.0, 0),
      TestPair(12.1, 4),
      TestPair(12.0, 4),
      TestPair(11.9, 3),
      TestPair(6.0, 3),
      TestPair(5.9, 2),
      TestPair(2.0, 2),
      TestPair(1.9, 1),
      TestPair(1.2, 1),
      TestPair(1.1, 0),
      TestPair(0.0, 0))
    pairs.foreach(p => {
      val sf = SofaFeatures(patientID, hadmID, icuStayID, chartTime,
        minResp, minPlatelets, p.input, minMeanBP, maxNorephinephrine, maxEpinephrine,
        maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine)
      val actual = SofaScore.liverFunctionScore(sf)
      println(s"$actual for $p")
      actual should be(p.expected)
    })
  }

  "coagulationScore" should "give expected results" in {
    val minResp, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine, maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine = 0.0
    val pairs: List[TestPair[Double]] = List(
      TestPair(-1.0, 0),
      TestPair(0.0, 4),
      TestPair(0.1, 4),
      TestPair(19.9, 4),
      TestPair(20.0, 3),
      TestPair(49.9, 3),
      TestPair(50.0, 2),
      TestPair(99.9, 2),
      TestPair(100.0, 1),
      TestPair(149.9, 1),
      TestPair(150.0, 0))
    pairs.foreach(p => {
      val sf = SofaFeatures(patientID, hadmID, icuStayID, chartTime,
        minResp, p.input, maxBilirubin, minMeanBP, maxNorephinephrine, maxEpinephrine,
        maxDopamine, maxDobutamine, minGCS, maxCreatinine, netUrine)
      val actual = SofaScore.coagulationScore(sf)
      println(s"$actual for $p")
      actual should be(p.expected)
    })
  }

}
