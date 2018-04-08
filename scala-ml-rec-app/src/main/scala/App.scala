package edu.neu.coe.csye7200.prodrec.learning

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object DataModelApp extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Santander Product Recommendation")
    .getOrCreate()

  import sparkSession.implicits._

  val trainDF = sparkSession.read
    .option("header","true")
    .option("inferSchema",true)
    .format("csv")
    .load("./trim_train.csv")

  // val colNos = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)

  //trainDF.select(colNos map (trainDF.columns andThen col): _*)

  val testDF = sparkSession.read
    .option("header","true")
    .option("inferSchema",true)
    .format("csv")
    .load("./trim_test.csv")

  trainDF.show()
  trainDF.na.drop()
  testDF.na.drop()

  val newTrainDF = trainDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")
  val newTestDF = testDF.drop("tipodom","cod_prov","convuemp","ult_fec_cli_lt")

  newTrainDF.show()*/

  //val (dataDFRaw, predictDFRaw) = loadData(args(0), args(1), sc)

  //newTrainDF.select(colNos map (df.columns andThen col): _*)

  val categoricalColNames = Seq("ind_empleado","pais_residencia","sexo","ind_nuevo","antiguedad",
    "indrel","indrel_1mes","tiprel_1mes","indresi","indext","conyuemp","canal_entrada","indfall",
    "ind_actividad_cliente","renta", "segmento")

  val featureIndexer = categoricalColNames.map {
    colName => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(newTrainDF)
  }

  print(trainDF.count())

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

  val targetIndexer = targetCols.map{
    colNames => new StringIndexer()
        .setInputCol(colNames)
        .setOutputCol(colNames + "Indexed")
        .fit(newTrainDF)
  }

  val numericColNames = Seq("ncodpers",
    "age", "ind_nuevo","antiguedad","indrel",
    "indrel_1mes",
    "ind_actividad_cliente", "renta")

  val idxdCategoricalColName = categoricalColNames.map(_ + "Indexed")
  val allIdxdColNames = numericColNames ++ idxdCategoricalColName
  val assembler = new VectorAssembler()
    .setInputCols(Array(allIdxdColNames: _*))
    .setOutputCol("Features")

  val idxdTargetColName = targetCols.map(_ + "Indexed")

  val assemblerT = new VectorAssembler()
    .setInputCols(Array(idxdTargetColName: _*))
    .setOutputCol("TargetIndex")

  val randomForest = new RandomForestClassifier()
    .setLabelCol("TargetIndex")
    .setFeaturesCol("Features")

  /*val labelConverter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictedLabel")
    .setLabels(targetIndexer.labels)*/
}