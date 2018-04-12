package edu.neu.coe.csye7200.prodrec.learning

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object DataModelApp extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Santander Product Recommendation")
    .getOrCreate()

  val (trainDF, testDF) = loadData(sparkSession)

  trainDF.show()

  trainDF.na.drop()
  testDF.na.drop()

  val newTrainDF = trainDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")
  val newTestDF = testDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")

  val numericColNames = Seq("ncodpers",
    "age", "ind_nuevo","antiguedad","indrel",
    "indrel_1mes", "ind_actividad_cliente", "renta")

  val categoricalColNames = Seq("ind_empleado","pais_residencia","sexo","ind_nuevo","antiguedad",
    "indrel","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall",
    "ind_actividad_cliente","renta", "segmento")

  val targetCols = Seq(
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

  val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName

  val featureIndexer = categoricalColNames.map {
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(newTrainDF)
  }

  val targetIndexer = targetCols.map{
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(newTrainDF)
  }

  val assembler = new VectorAssembler()
    .setInputCols(Array(allIdxdColNames: _*))
    .setOutputCol("Features")

  val idxdTargetColName = targetCols.map(_ + "Indexed")

  val featureIndexerr = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(newTrainDF)

  // Train a RandomForest model.
  val randomForest = new RandomForestClassifier()
    .setLabelCol("targetIndexer")
    .setFeaturesCol("Features")

  // Convert indexed labels back to original labels
  val labConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(targetIndexer.labels)

  // Chain indexer and forest in a Pipeline.
  val pipeline = new Pipeline().setStages(Array.concat(
    featureIndexer.toArray,
    Array(assembler, randomForest, labConverter)
  ))

  // Train model. This also runs the indexers.
  val model = pipeline.fit(newTrainDF)

  // Make predictions.
  val predictions = model.transform(newTestDF)

  // Select example rows to display.
  predictions.select("predictedLabel", "target", "features").show(5)

  // Select (prediction, true label) and compute test error.
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
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