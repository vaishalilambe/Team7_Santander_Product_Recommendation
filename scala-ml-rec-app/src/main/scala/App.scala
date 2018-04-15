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
    trainDF("age"),
    trainDF("seniority"),
    trainDF("isCustomerActive")
  )

  val testDFC:DataFrame = testDF.select(
    testDF("age"),
    testDF("seniority")
  )

  //val Array(trainingData, testData) = trainDFC.randomSplit(Array(0.7, 0.3))

  print("Clean data: "+trainDFC.show)

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

  /*val newTrainDF = trainDF.select("ncodpers",
    "ind_nuevo","antiguedad","indrel",
    "indrel_1mes", "ind_actividad_cliente", "renta")

  val newTestDF = testDF.select("ncodpers",
    "age", "ind_nuevo","antiguedad","indrel",
    "indrel_1mes", "ind_actividad_cliente", "renta")*/

/*
  val numericColNames = Seq("ncodpers",
    "age", "ind_nuevo","antiguedad","indrel",
    "indrel_1mes", "ind_actividad_cliente", "renta")*/

  /*val categoricalColNames = Seq("ind_empleado","pais_residencia","sexo","ind_nuevo","antiguedad",
    "indrel","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall",
    "ind_actividad_cliente","renta", "segmento")*/

  /*val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName*/

  val numericColNames = Seq("age", "seniority")

  /*val featureIndexer = numericColNames.map {
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName+"Indexed")
        .fit(trainDFC)
  }*/

  val labelIndexer = new StringIndexer()
    .setInputCol("isCustomerActive")
    .setOutputCol("productIndexed")
    .fit(trainDFC)
    .setHandleInvalid("skip")

  labelIndexer.transform(trainDFC).show
  //print("The value is: "+labelIndexer.transform(trainDFC).show())

  val assembler:VectorAssembler = new VectorAssembler()
    .setInputCols(Array("age", "seniority"))
    .setOutputCol("Features")

  val output = assembler.transform(trainDFC)

  output.select("Features", "isCustomerActive").show

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
    Array(labelIndexer, assembler, randomForest, labelConverter))

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainDFC)

  // Make predictions.
  val predictions = model.transform(testDF)

  // Select example rows to display.
  predictions.select("predictedLabel", "productIndexed", "Features").show(5)

  // Select (prediction, true label) and compute test error.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("productIndexed")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val accuracy = evaluator.evaluate(predictions)
  println(s"Test Error = ${(1.0 - accuracy)}")

  val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
  println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

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