package edu.neu.coe.csye7200.prodrec.learning

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{DateTime, Seconds}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.classification.NaiveBayes
//import org.apache.spark.ml.classification.DecisionTreeClassifier

object DataModelApp extends App {

  val SANTANDER_PRODUCT_RECOMMENDATION_APP = "Santander Product Recommendation with Random Forest Classification"
  val SET_UP_MESSAGE_COMPLETION = "Spark Set Up Complete"
  val CREATE_MODEL_MESSAGE = "Creating Random Forest Model"
  val SAVE_MODEL_MESSAGE = "Saving the Model"
  val CREATE_PIPELINE_MESSAGE = "Creating Pipleline"

  val filePath = "./dataset/clean_data.csv"
  val modelSavePath = "./dataset/spark-random-forest-model"
  val maxBin:Int = 60
  val splitSeed:Int = 5043
  val metricName = "weightedPrecision"
  val featuresColName = "Features"
  val productColName = "product"
  val productIndexedColName = "productIndexed"
  val predictColName = "prediction"
  val predictOutputColName = "predictedLabel"

  val numericColNames:Seq[String] = Seq("code","age","income")
  val categoricalColNames:Seq[String] = Seq(
    "gender",
    "employmentStatus",
    "customerAddrProvinceName",
    "customerRelationTypeFirstMonth",
    "customerType",
    "deceasedIndex"
  )

  val logger = getLogger()

  logger.info(s"Starting up $SANTANDER_PRODUCT_RECOMMENDATION_APP")

  val sparkSession = SparkSession.builder.
    master("local")
    .appName(SANTANDER_PRODUCT_RECOMMENDATION_APP)
    .getOrCreate()

  import sparkSession.implicits._

  logger.info(SET_UP_MESSAGE_COMPLETION)

  //Loading the train data
  val trainDF:DataFrame = loadCleanedData(sparkSession, filePath)

  //Based on feature engineering, filtering best features for the model
  val filteredData: DataFrame = filterData(trainDF)

  //filteredData.printSchema

  //Converting categorical columns to numeric
  val categoricalFeatureIndexer:Seq[StringIndexerModel] = convertCategoricalToIndexes(categoricalColNames, filteredData)

  // Appending categorical index columns and numeric columns
  val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName

  //Create Index for Target column
  val labelIndexer:StringIndexerModel = new StringIndexer()
    .setInputCol(productColName)
    .setOutputCol(productIndexedColName)
    .fit(filteredData)
    .setHandleInvalid("skip")

  // Split training and test data
  val Array(trainingData, testData) = filteredData.randomSplit(Array(0.7, 0.3), splitSeed)

  //Convert all the features to Vector
  val assembler:VectorAssembler = new VectorAssembler()
    .setInputCols(Array(allIdxdColNames: _*))
    .setOutputCol(featuresColName)

  logger.info(CREATE_MODEL_MESSAGE)

  /*val nb = new NaiveBayes()
    .setLabelCol("productIndexed")
    .setFeaturesCol("Features")*/

  /*val dt = new DecisionTreeClassifier()
    .setLabelCol(productIndexedColName)
    .setFeaturesCol(featuresColName)
    .setMaxBins(maxBin)*/

  // Train a RandomForest model.
  val randomForest:RandomForestClassifier = new RandomForestClassifier()
    .setLabelCol("productIndexed")
    .setFeaturesCol("Features")
    .setMaxBins(maxBin)

  // Convert indexed labels back to original labels
  val labelConverter:IndexToString = new IndexToString()
    .setInputCol(predictColName)
    .setOutputCol(predictOutputColName)
    .setLabels(labelIndexer.labels)

  logger.info(CREATE_MODEL_MESSAGE)

  // Chain indexer and forest in a Pipeline
  val pipeline:Pipeline = new Pipeline().setStages(
    categoricalFeatureIndexer.toArray ++ Array(labelIndexer, assembler, randomForest, labelConverter))

  val startRandomForest = DateTime.now

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainingData)

  logger.info(SAVE_MODEL_MESSAGE)

  // Save model for new data prediction
  model.write.overwrite().save(modelSavePath)

  val endRandomForest = DateTime.now

  // Make predictions.
  val predictions = model.transform(testData)

  // Show predictions for the model
  //predictions.printSchema()
  predictions.select(predictOutputColName, featuresColName).show()

  val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(productIndexedColName)
      .setPredictionCol(predictColName)
      .setMetricName(metricName)

  val precision = precisionEvaluator.evaluate(predictions)
  println(s"Precision = ${(precision)}")

  // Save the predictions
  //savePredictions(predictions)

  val predictionTime = Seconds.secondsBetween(startRandomForest, endRandomForest).getSeconds
  println(s"Total Prediction Time: $predictionTime seconds")

  //val rfModel = model.stages(3).asInstanceOf[RandomForestClassificationModel]
  //println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

  def getLogger(): Logger = {
    val logger = Logger.getLogger("org")
    logger
  }

  def loadCleanedData(sc : SparkSession, filePath: String): DataFrame = {

    val trainDF = sc.read
      .option("header","true")
      .option("inferSchema",true)
      .format("csv")
      .load(filePath)
      .cache()

    trainDF
  }

  def filterData(trainDF : DataFrame): DataFrame ={
    val filteredData : DataFrame = trainDF
      .select(
        trainDF("code"),
        trainDF("gender"),
        trainDF("age"),
        trainDF("income"),
        trainDF("employmentStatus"),
        trainDF("customerType"),
        trainDF("deceasedIndex"),
        trainDF("countryOfResidence"),
        trainDF("customerRelationTypeFirstMonth"),
        trainDF("customerResidenceIndex"),
        trainDF("customerAddrProvinceName"),
        trainDF("product")
      )

    //import org.apache.spark.sql.functions._

    // Count Top 10 products and filter the data for these products. These top products covers 95% of the data.
    // This helped in increasing the Precision of the model

    //filteredData.groupBy("product").count().orderBy(desc("count")).show

    //val topProducts = List("[3]", "[3,8]", "[3,24]", "[8]", "[3,13]", "[6]", "[13]", "[3,9]", "[3,12]", "[5,22,23,24]")
    //filteredData.filter($"product".isin(topProducts:_*)).show()

    filteredData
  }

  def convertCategoricalToIndexes(catColNames:Seq[String], trainingData:DataFrame) = {
    val categoricalFeatureIndexer = catColNames.map {
      colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(trainingData)
        .setHandleInvalid("skip")
    }

    categoricalFeatureIndexer
  }

  def savePredictions(predictions:DataFrame): Unit ={
    predictions
      .select("code", "predictedLabel")
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save("./dataset/predctions.csv")
  }
}
