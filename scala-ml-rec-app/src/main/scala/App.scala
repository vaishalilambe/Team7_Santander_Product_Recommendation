package edu.neu.coe.csye7200.prodrec.learning

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.types.IntegerType

object DataModelApp extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Santander Product Recommendation")
    .getOrCreate()

  val (trainDF, testDF) = loadData(sparkSession)

  trainDF.show()
  trainDF.printSchema()

  trainDF.na.drop()
  testDF.na.drop()

  val newTrainDF = trainDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")
  val newTestDF = testDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")

  val trainDFC = newTrainDF.select(
    newTrainDF("ncodpers").cast(IntegerType).as("ncodpers"),
    newTrainDF("age").cast(IntegerType).as("age"),
    newTrainDF("renta").cast(IntegerType).as("renta"),
    newTrainDF("antiguedad").cast(IntegerType).as("antiguedad"),
    newTrainDF("ind_ahor_fin_ult1"),
    newTrainDF("ind_aval_fin_ult1"),
    newTrainDF("ind_cco_fin_ult1"),
    newTrainDF("ind_aval_fin_ult1"),
    newTrainDF("ind_cco_fin_ult1"),
    newTrainDF("ind_cder_fin_ult1"),
    newTrainDF("ind_cno_fin_ult1"),
    newTrainDF("ind_ctju_fin_ult1"),
    newTrainDF("ind_ctma_fin_ult1"),
    newTrainDF("ind_ctop_fin_ult1"),
    newTrainDF("ind_ctpp_fin_ult1"),
    newTrainDF("ind_deco_fin_ult1"),
    newTrainDF("ind_deme_fin_ult1"),
    newTrainDF("ind_dela_fin_ult1"),
    newTrainDF("ind_ecue_fin_ult1"),
    newTrainDF("ind_fond_fin_ult1"),
    newTrainDF("ind_hip_fin_ult1"),
    newTrainDF("ind_plan_fin_ult1"),
    newTrainDF("ind_pres_fin_ult1"),
    newTrainDF("ind_reca_fin_ult1"),
    newTrainDF("ind_tjcr_fin_ult1"),
    newTrainDF("ind_valo_fin_ult1"),
    newTrainDF("ind_viv_fin_ult1"),
    newTrainDF("ind_nomina_ult1"),
    newTrainDF("ind_nom_pens_ult1"),
    newTrainDF("ind_recibo_ult1")
  )

  /*val trainDFCN = newTrainDF.select(
    newTrainDF("ncodpers").cast(IntegerType).as("ncodpers"),
    newTrainDF("age").cast(IntegerType).as("age"),
    newTrainDF("renta").cast(IntegerType).as("renta"),
    newTrainDF("antiguedad").cast(IntegerType).as("antiguedad"),
    newTrainDF("ind_ahor_fin_ult1")
  )*/

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

  val targetCols = Array(
    "ind_ahor_fin_ult1",
    "ind_aval_fin_ult1",
    "ind_cco_fin_ult1",
    "ind_aval_fin_ult1",
    "ind_cco_fin_ult1",
    "ind_cder_fin_ult1",
    "ind_cno_fin_ult1",
    "ind_ctju_fin_ult1",
    "ind_ctma_fin_ult1",
    "ind_ctop_fin_ult1",
    "ind_ctpp_fin_ult1",
    "ind_deco_fin_ult1",
    "ind_deme_fin_ult1",
    "ind_dela_fin_ult1",
    "ind_ecue_fin_ult1",
    "ind_fond_fin_ult1",
    "ind_hip_fin_ult1",
    "ind_plan_fin_ult1",
    "ind_pres_fin_ult1",
    "ind_reca_fin_ult1",
    "ind_tjcr_fin_ult1",
    "ind_valo_fin_ult1",
    "ind_viv_fin_ult1",
    "ind_nomina_ult1",
    "ind_nom_pens_ult1",
    "ind_recibo_ult1"
  )

  /*val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName*/

  val numericColNames = Seq("ncodpers", "age", "antiguedad", "renta")

  val targetIndexer = targetCols.map{
    colName => new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "_Indexed")
      .fit(trainDFC)
  }

  val featureIndexer = numericColNames.map {
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(trainDFC)
  }

  /*val targetIndexer = new StringIndexer()
       .setInputCol("ind_ahor_fin_ult1")
       .setOutputCol("ind_ahor_fin_ult1Indexed")
       .fit(trainDFCN)*/

  val assembler = new VectorAssembler()
    .setInputCols(Array(numericColNames: _*))
    .setOutputCol("features")

  val tassembler = new VectorAssembler()
    .setInputCols(Array(targetCols: _*))
    .setOutputCol("label")

  // Train a RandomForest model.
  val randomForest = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")

  // Convert indexed labels back to original labels
  val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(targetCols)

  // Chain indexer and forest in a Pipeline.
  val pipeline = new Pipeline().setStages(
    featureIndexer.toArray ++ targetIndexer ++ Array(assembler, tassembler,
    randomForest, labelConverter))

  // Train model. This also runs the indexers.
  val model = pipeline.fit(trainDFC)

  // Make predictions.
  val predictions = model.transform(newTestDF)

  //println(s"The value of prediction is : $predictions")

  // Select example rows to display.
  predictions.select("predictedLabel", "label", "features").show(5)

  // Select (prediction, true label) and compute test error.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
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
      .load("./dataset/trim_train.csv")

    val testDF = sc.read
      .option("header","true")
      .option("inferSchema",true)
      .format("csv")
      .load("./dataset/trim_test.csv")

    (trainDF, testDF)
  }
}