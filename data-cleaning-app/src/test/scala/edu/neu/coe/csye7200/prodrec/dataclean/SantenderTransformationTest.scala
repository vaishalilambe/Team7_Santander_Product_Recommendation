package edu.neu.coe.csye7200.prodrec.dataclean

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import edu.neu.coe.csye7200.prodrec.dataclean.pipeline.TransfomationLogic
import org.scalatest.{FlatSpec,Matchers}

class SantenderTransformationTest extends FlatSpec with DataFrameSuiteBase with Matchers {


  behavior of "DataFrame Transformation"


  it should "work for Transformation" in {

    val sqlCx = sqlContext
    import sqlCx.implicits._

    val input1 = sc.parallelize(Seq(Some(10),Some(20),Some(30),None)).toDF(Seq("age"): _*)
    val expectedOutput1 = sc.parallelize(Seq(10,20,30,20)).toDF(Seq("age"): _*)
    val actualOutput1 = TransfomationLogic.fixAge(input1)
    assertDataFrameEquals(expectedOutput1, actualOutput1)

    val input2 = sc.parallelize(Seq(Some(10.0),Some(20.0),Some(30.0),None)).toDF(Seq("income"): _*)
    val expectedOutput2 = sc.parallelize(Seq(10.0,20.0,30.0,20.0)).toDF(Seq("income"): _*)
    val actualOutput2 = TransfomationLogic.replaceNullWithAvg(input2)
    assertDataFrameEquals(expectedOutput2, actualOutput2)

  }

  behavior of "UDF functions"

  it should "work for emptyToGenderRation" in {
    TransfomationLogic.emptyToGenderRation("V") shouldBe "V"
    TransfomationLogic.emptyToGenderRation("") should contain oneOf('V', 'H')
  }

  it should "work for emptyToSNRatio" in {
    TransfomationLogic.emptyToSNRatio("S") shouldBe "S"
    TransfomationLogic.emptyToSNRatio("") should contain oneOf('S', 'N')
  }

  it should "work for emptyToIARatio" in {
    TransfomationLogic.emptyToIARatio("A") shouldBe "A"
    TransfomationLogic.emptyToIARatio("") should contain oneOf('A', 'I')
  }

  it should "work for emptyToSNRatioRev" in {
    TransfomationLogic.emptyToSNRatioRev("S") shouldBe "S"
    TransfomationLogic.emptyToSNRatioRev("") should contain oneOf('S', 'N')
  }

  it should "work for emptyToUnknown" in {
    TransfomationLogic.emptyToUnknown("X") shouldBe "X"
    TransfomationLogic.emptyToUnknown("") shouldBe "Unknown"
  }

  it should "work for removeQuotes" in {
    TransfomationLogic.removeQuotes("\"Hello\"") shouldBe "Hello"
    TransfomationLogic.removeQuotes("Hello") shouldBe "Hello"
  }

}
