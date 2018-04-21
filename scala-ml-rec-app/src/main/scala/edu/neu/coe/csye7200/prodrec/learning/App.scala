package edu.neu.coe.csye7200.prodrec.learning

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object DataModelApp extends App {

  val SANTANDER_PRODUCT_RECOMMENDATION_APP = "Santander Product Recommendation with Random Forest Classification"
  val SET_UP_MESSAGE_COMPLETION = "Spark Set Up Complete"
  val filePath = "./dataset/part-00000-a8e7bf79-9817-48e0-bc58-825c29c3d30d-c000.csv"

  val numericColNames:Seq[String] = Seq("code","age","income")
  val categoricalColNames:Seq[String] = Seq(
    "gender",
    "employmentStatus",
    "customerAddrProvinceName",
    "customerRelationTypeFirstMonth",
    "customerType",
    "deceasedIndex"
  )

  val topProducts = Seq("[3]", "[3,8]", "[3,24]", "[8]", "[3,13]", "[6]", "[13]", "[3,9]",
    "[3,12]", "[5,22,23,24]")

  val logger = getLogger()

  logger.info(s"Starting up $SANTANDER_PRODUCT_RECOMMENDATION_APP")

  val sparkSession = SparkSession.builder.
    master("local")
    .appName(SANTANDER_PRODUCT_RECOMMENDATION_APP)
    .getOrCreate()

  logger.info(SET_UP_MESSAGE_COMPLETION)

  //Loading the train data

  val trainDF:DataFrame = loadCleanedData(sparkSession, filePath)

  trainDF.show()
  trainDF.printSchema()

  val filteredData = filterData(trainDF)

  // Split training and test data
  //val splitSeed = 5043
  val Array(trainingData, testData) = filteredData.randomSplit(Array(0.9, 0.1))

  logger.info("Training data :")
  trainingData.show

  logger.info("Test data :")
  testData.show

  filteredData.printSchema

  //Converting categorical columns to numeric
  val categoricalFeatureIndexer = convertCategoricalToIndexes(categoricalColNames)

  // Appending categorical index columns and numeric columns
  val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName

  //Create Index for Target column
  val labelIndexer = new StringIndexer()
    .setInputCol("product")
    .setOutputCol("productIndexed")
    .fit(trainingData)
    .setHandleInvalid("skip")

  //Convert all the features to Vector
  val assembler:VectorAssembler = new VectorAssembler()
    .setInputCols(Array(allIdxdColNames: _*))
    .setOutputCol("Features")

  logger.info("Creating Random Forest Model")

  // Train a RandomForest model.
  val randomForest = new RandomForestClassifier()
    .setLabelCol("productIndexed")
    .setFeaturesCol("Features")
    .setMaxBins(60)

  // Convert indexed labels back to original labels
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(labelIndexer.labels)

  logger.info("Creating Random Forest Model")
  // Chain indexer and forest in a Pipeline.
  val pipeline = new Pipeline().setStages(
    categoricalFeatureIndexer.toArray ++ Array(labelIndexer, assembler, randomForest, labelConverter))

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainingData)

  logger.info("Saving the Model")
  //Save model
    model.write.overwrite().save("./dataset/spark-random-forest-model")

  // Make predictions.
  val predictions = model.transform(testData)

  // Select example rows to display.

  //predictions.printSchema()
  predictions.select("predictedLabel", "productIndexed", "Features", "probability").show()

  val precisionEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("productIndexed")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

  val precision = precisionEvaluator.evaluate(predictions)
  logger.info(s"Precision = ${(precision)}")

  predictions
    .select("code", "predictedLabel")
    .coalesce(1)
    .write
    .format("csv")
    .option("header", "true")
    .save("./dataset/predctions.csv")

  //val rfModel = model.stages(3).asInstanceOf[RandomForestClassificationModel]
  //println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

  def getLogger(): Logger = {
    val logger = Logger.getLogger("some")
    logger
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

    filteredData
  }

  def convertCategoricalToIndexes(catColNames:Seq[String]) = {
    val categoricalFeatureIndexer = catColNames.map {
      colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(trainingData)
        .setHandleInvalid("skip")
    }

    categoricalFeatureIndexer
  }

  def createPipeline(){}
  def savePredictions(){}

  def loadCleanedData(sc : SparkSession, filePath: String): DataFrame = {

    //import sparkSession.implicits._

    val trainDF = sc.read
      .option("header","true")
      .option("inferSchema",true)
      .format("csv")
      .load(filePath)
      .cache()

    trainDF
  }
}