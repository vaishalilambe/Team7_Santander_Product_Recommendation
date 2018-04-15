package edu.neu.coe.csye7200.prodrec.learning

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object DataModelApp extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Santander Product Recommendation")
    .getOrCreate()

  val (trainDF, testDF):(DataFrame,DataFrame) = loadData(sparkSession)

  trainDF.show()
  trainDF.printSchema()

  val trainDFC:DataFrame = trainDF.select(
    trainDF("code"),
    trainDF("age"),
    trainDF("seniority"),
    trainDF("income"),
    trainDF("employmentStatus"),
    trainDF("countryOfResidence"),
    trainDF("gender"),
    trainDF("customerRelationTypeFirstMonth"),
    trainDF("customerResidenceIndex"),
    trainDF("isCustomerActive")
  )

  val testDFC:DataFrame = testDF.select(
    testDF("age"),
    testDF("seniority"),
    testDF("income")
  )

  val Array(trainingData, testData) = trainDFC.randomSplit(Array(0.7, 0.3))

  trainingData.show
  testData.show

  //trainDFC.filter("income is null").show
  /*val trainDFCN = newTrainDF.select(
    newTrainDF("ncodpers").cast(IntegerType).as("ncodpers"),
    newTrainDF("age").cast(IntegerType).as("age"),
    newTrainDF("renta").cast(IntegerType).as("renta"),
    newTrainDF("antiguedad").cast(IntegerType).as("antiguedad"),
    newTrainDF("ind_ahor_fin_ult1")
  )*/

  testDFC.show
  trainDFC.printSchema

  val numericColNames = Seq("code","age", "seniority","income")
  val categoricalColNames = Seq("employmentStatus",
    "countryOfResidence",
    "gender",
    "customerRelationTypeFirstMonth",
    "customerResidenceIndex"
  )

  val featureIndexer = categoricalColNames.map {
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(trainingData)
        .setHandleInvalid("skip")
  }

  val labelIndexer = new StringIndexer()
    .setInputCol("isCustomerActive")
    .setOutputCol("productIndexed")
    .fit(trainingData)
    .setHandleInvalid("skip")

  labelIndexer.transform(trainingData).show
  print("The features unindexed are ============>")
  featureIndexer.map{x => x.transform(trainingData).show(1)}

  val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName

  val assembler:VectorAssembler = new VectorAssembler()
    .setInputCols(Array(allIdxdColNames: _*))
    .setOutputCol("Features")

  //val output = assembler.transform(trainingData)

  //output.select("Features", "isCustomerActive").show

  // Train a RandomForest model.
  val randomForest = new RandomForestClassifier()
    .setLabelCol("productIndexed")
    .setFeaturesCol("Features")

  // Convert indexed labels back to original labels
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

  // Chain indexer and forest in a Pipeline.
  val pipeline = new Pipeline().setStages(
    featureIndexer.toArray ++ Array(labelIndexer, assembler, randomForest, labelConverter))

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainingData)

  //Save model
    //model.write.overwrite().save("/tmp/spark-random-forest-model")

  // Make predictions.
  val predictions = model.transform(testData)

  // Select example rows to display.

  //predictions.printSchema()
  predictions.select("predictedLabel", "productIndexed", "Features", "probability").show()

  // Select (prediction, true label) and compute test error.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("productIndexed")
    .setPredictionCol("prediction")
    .setMetricName("weightedPrecision")

  val accuracy = evaluator.evaluate(predictions)
  println(s"Precision = ${(accuracy)}")

  predictions
    .select("code", "predictedLabel")
    .coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .save("./dataset/predctions.csv")

  //val rfModel = model.stages(3).asInstanceOf[RandomForestClassificationModel]
  //println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

  def loadData(sc : SparkSession): (DataFrame, DataFrame) = {

    import sparkSession.implicits._

    val trainDF = sc.read
      .option("header","true")
      .option("inferSchema",true)
      .format("csv")
      .load("./dataset/trim_trait.csv")

    val testDF = sc.read
      .option("header","true")
      .option("inferSchema",true)
      .format("csv")
      .load("./dataset/trim_trait_test.csv")

    (trainDF, testDF)
  }
}