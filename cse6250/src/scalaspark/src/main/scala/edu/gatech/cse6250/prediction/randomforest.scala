package edu.gatech.cse6250.prediction

import edu.gatech.cse6250.model._
import edu.gatech.cse6250.helper.SparkHelper
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object RFModel {
  def fitAndSave(svmPath: String, modelPath: String) {
    val trainingData = SparkHelper.spark.read.format("libsvm").load(svmPath)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainingData)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(trainingData)

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(100)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxBins, Array(32, 48, 64))
      .addGrid(rf.numTrees, Array(10, 100, 200))
      .addGrid(rf.maxDepth, Array(4, 5, 6))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val cvModel = cv.fit(trainingData)

    println("Average metrics: ")
    cvModel.avgMetrics.foreach(println)

    val rfModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    // Save and load model
    rfModel.save(modelPath)
  }

  def loadAndPredict(modelPath: String, svmPath: String) {
    val model = PipelineModel.load(modelPath)
    val testData = SparkHelper.spark.read.format("libsvm").load(svmPath)

    val predictions = model.transform(testData)

    val evaluator = new BinaryClassificationEvaluator()
    val auc = evaluator.evaluate(predictions)

    println(s"AUC = $auc")
    val labels = testData.select("label").rdd.map(r => r.getDouble(0))
      .zipWithIndex.map(p => (p._2, p._1))
    val predLabels = predictions.select("predictedLabel").rdd.map(r => r.getString(0).toDouble)
      .zipWithIndex.map(p => (p._2, p._1))
    val predAndTrue = predLabels.join(labels).map { case (idx, (pred, actual)) => (pred, actual) }

    val metrics = new MulticlassMetrics(predAndTrue)
    println("Confusion Matrix:")
    println(metrics.confusionMatrix)

  }
}
