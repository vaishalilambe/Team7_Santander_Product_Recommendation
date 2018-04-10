package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.io.DataParser
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import shapeless.{HList, ::, HNil}
import org.apache.spark.sql.functions.col

object Pipeline {

  def run(input: String, ss: SparkSession): DataFrame = {

    import ss.implicits._

    //Step 1: Get Dataset[String]
    val stringDS = DataParser.getStringDS(input,ss)

    //Step 2: Parse Dataset[String] to Dataset[SantanderRecord]
    val classDS = DataParser.stringDStoClassDS(stringDS, ss)

    //Step 3: Filter for customer who failed parsing Customer.code
    val classDS1 = classDS.filter(d => d.customerInfo.code != None)

//    for(x <- df.columns)
////      for(y <- df.select(col(x)).columns)
////        println(y)
//     df.select(col(x)).map( x=>x._1 ).show()

    //TODO: Find better way to do this
    val df = classDS1.toDF()
    val newDf = df.withColumn("code",col("customerInfo.code"))
      .withColumn("employmentStatus",col("customerInfo.employmentStatus"))
      .withColumn("countryOfResidency",col("customerInfo.countryOfResidence"))
      .withColumn("gender",col("customerInfo.gender"))
      .withColumn("age",col("customerInfo.age"))
      .withColumn("income",col("customerInfo.income"))
      .drop("customerInfo")

    newDf.show()


    //Step : Create DataFrame: Rename Columns and manually flatter from Dataset[SantanderRecord]
    val colNames = Seq("code","employmentStatus","countryOfResidency","gender","age","income",
      "customerType","joinDate","isCustomerAtMost6MonthOld","seniority","isPrimaryCustomer","customerTypeFirstMonth",
      "customerRelationTypeFirstMonth","customerResidenceIndex","customerForeignIndex","channelOfJoin","deceasedIndex","provinceName","isCustomerActive",
      "savingAcc","guarantees","currentAcc","derivedAcc","payrollAcc","juniorAcc","moreParticularAcc","particularAcc","particularPlusAcc",
      "shortTermDeposit","midTermDeposit","longTermDeposit","eAccount","funds","mortgage","pensionPlan","loan","taxes","creditCard",
      "securities","homeAcc","payrollNom","pensionNom","directDebit"
    )

//    val df = classDS1.map(x => (x.customerInfo.code,x.customerInfo.employmentStatus,x.customerInfo.countryOfResidence,x.customerInfo.gender,x.customerInfo.age,x.customerInfo.income,
//      x.accountInfo.customerType,x.accountInfo.joinDate,x.accountInfo.isCustomerAtMost6MonthOld,x.accountInfo.seniority,x.accountInfo.isPrimaryCustomer,x.accountInfo.customerTypeFirstMonth,
//      x.accountInfo.customerRelationTypeFirstMonth,x.accountInfo.customerResidenceIndex,x.accountInfo.customerForeignIndex,x.accountInfo.channelOfJoin,x.accountInfo.deceasedIndex,x.accountInfo.customerAddrProvinceName,x.accountInfo.isCustomerActive,
//      x.productInfo.savingAcc,x.productInfo.guarantees,x.productInfo.currentAcc,x.productInfo.derivedAcc,x.productInfo.payrollAcc,x.productInfo.juniorAcc,x.productInfo.moreParticularAcc,x.productInfo.particularAcc,x.productInfo.particularPlusAcc,
//      x.productInfo.shortTermDeposit,x.productInfo.midTermDeposit,x.productInfo.longTermDeposit,x.productInfo.eAccount,x.productInfo.funds,x.productInfo.mortgage,x.productInfo.pensionPlan,x.productInfo.loan,x.productInfo.taxes,x.productInfo.creditCard,
//      x.productInfo.securities,x.productInfo.homeAcc,x.productInfo.payrollNom,x.productInfo.pensionNom,x.productInfo.directDebit
//    )).toDF(colNames: _*)
//
////    df.show()
////
////    val df = classDS1
////      .flatMap()
////    val df = classDS1.map( x => x.customerInfo.income :: x.customerInfo.gender)
//    df.show()

    classDS1.toDF()
  }

}