package edu.gatech.cse6250.phenotyping

import edu.gatech.cse6250.model._

object SofaScorer {
  // https://github.com/MIT-LCP/mimic-code/blob/master/concepts/severityscores/sofa.sql
  def respirationScore(sofaFeatures: SofaFeatures): Int = {
    if (sofaFeatures.minRespRatio < 0) {
      // If value is missing, impute with 0
      0
    } else if (sofaFeatures.minRespRatio < 67.0) {
      4
    } else if (sofaFeatures.minRespRatio < 142.0) {
      3
    } else if (sofaFeatures.minRespRatio < 221.0) {
      2
    } else if (sofaFeatures.minRespRatio < 301.0) {
      1
    } else {
      0
    }
  }

  def renalScore(sofaFeatures: SofaFeatures): Int = {
    if (sofaFeatures.maxCreatinine < 0 && sofaFeatures.netUrine < 0) {
      // If values are missing, impute with 0
      0
    } else if (sofaFeatures.maxCreatinine >= 5.0 ||
      (sofaFeatures.netUrine > 0.0 && sofaFeatures.netUrine < 200.0)) {
      4
    } else if ((sofaFeatures.maxCreatinine >= 3.5 && sofaFeatures.maxCreatinine < 5.0) ||
      (sofaFeatures.netUrine > 0.0 && sofaFeatures.netUrine < 500.0)) {
      3
    } else if (sofaFeatures.maxCreatinine >= 2.0 && sofaFeatures.maxCreatinine < 3.5) {
      2
    } else if (sofaFeatures.maxCreatinine >= 1.2 && sofaFeatures.maxCreatinine < 2.0) {
      1
    } else {
      0
    }
  }

  def neurologicScore(sofaFeatures: SofaFeatures): Int = {
    // If value is missing, impute with 0
    if (sofaFeatures.minGCS < 0) {
      0
    } else if (sofaFeatures.minGCS < 6) {
      4
    } else if (sofaFeatures.minGCS < 9) {
      3
    } else if (sofaFeatures.minGCS < 12) {
      2
    } else if (sofaFeatures.minGCS < 14) {
      1
    } else {
      0
    }
  }

  def cardiovascularScore(sofaFeatures: SofaFeatures): Int = {
    if (sofaFeatures.maxDopamine > 15.0 ||
      sofaFeatures.maxEpinephrine > 0.1 ||
      sofaFeatures.maxNorepinephrine > 0.1) {
      4
    } else if (sofaFeatures.maxDopamine > 5.0 ||
      (sofaFeatures.maxEpinephrine > 0.0 && sofaFeatures.maxEpinephrine <= 0.1) ||
      (sofaFeatures.maxNorepinephrine > 0.0 && sofaFeatures.maxNorepinephrine <= 0.1)) {
      3
    } else if (sofaFeatures.maxDopamine > 0.0 ||
      sofaFeatures.maxDobutamine > 0.0) {
      2
    } else if (sofaFeatures.minMeanBP > 0.0 && sofaFeatures.minMeanBP < 70.0) {
      1
    } else {
      0
    }
  }

  def liverFunctionScore(sofaFeatures: SofaFeatures): Int = {
    // The chart shows > 12.0, but this leaves a gap at 12.0.
    // The SQL query uses >= 12.0.
    if (sofaFeatures.maxBilirubin >= 12.0) {
      4
    } else if (sofaFeatures.maxBilirubin >= 6.0) {
      3
    } else if (sofaFeatures.maxBilirubin >= 2.0) {
      2
    } else if (sofaFeatures.maxBilirubin >= 1.2) {
      1
    } else {
      0
    }
  }

  def coagulationScore(sofaFeatures: SofaFeatures): Int = {
    // If value is missing, impute with 0
    if (sofaFeatures.minPlatelets < 0) {
      0
    } else if (sofaFeatures.minPlatelets < 20.0) {
      4
    } else if (sofaFeatures.minPlatelets < 50.0) {
      3
    } else if (sofaFeatures.minPlatelets < 100.0) {
      2
    } else if (sofaFeatures.minPlatelets < 150.0) {
      1
    } else {
      0
    }
  }

  def combinedScore(sofaFeatures: SofaFeatures): Int = {
    respirationScore(sofaFeatures) +
      renalScore(sofaFeatures) +
      neurologicScore(sofaFeatures) +
      cardiovascularScore(sofaFeatures) +
      liverFunctionScore(sofaFeatures) +
      coagulationScore(sofaFeatures)
  }
}
