package edu.neu.coe.csye7200.prodrec.dataclean.model

import scala.util.{Failure, Success, Try}
import scala.collection.mutable.TreeSet

case class Product(product: String)

object Product {

  def intToBool(number: String): Boolean = Try(number.trim.toInt) match {
    case Success(1) => true
    case Success(0) => false
    case Success(x) => false
    case Failure(e) => false
  }

  def apply(
             savingAcc: String,
             guarantees: String,
             currentAcc: String,
             derivedAcc: String,
             payrollAcc: String,
             juniorAcc: String,
             moreParticularAcc: String,
             particularAcc: String,
             particularPlusAcc: String,
             shortTermDeposit: String,
             midTermDeposit: String,
             longTermDeposit: String,
             eAccount: String,
             funds: String,
             mortgage: String,
             pensionPlan: String,
             loan: String,
             taxes: String,
             creditCard: String,
             securities: String,
             homeAcc: String,
             payrollNom: String,
             pensionNom: String,
             directDebit: String
           ): Product = {

    val builder = StringBuilder.newBuilder
    var set = TreeSet[Int]()

      if (intToBool(savingAcc)) set += 1
      if (intToBool(guarantees)) set += 2
      if (intToBool(currentAcc)) set += 3
      if (intToBool(derivedAcc)) set += 4
      if (intToBool(payrollAcc)) set += 5
      if (intToBool(juniorAcc)) set += 6
      if (intToBool(moreParticularAcc)) set += 7
      if (intToBool(particularAcc)) set += 8
      if (intToBool(particularPlusAcc)) set += 9
      if (intToBool(shortTermDeposit)) set += 10
      if (intToBool(midTermDeposit)) set += 11
      if (intToBool(longTermDeposit)) set += 12
      if (intToBool(eAccount)) set += 13
      if (intToBool(funds)) set += 14
      if (intToBool(mortgage)) set += 15
      if (intToBool(pensionPlan)) set += 16
      if (intToBool(loan)) set += 17
      if (intToBool(taxes)) set += 18
      if (intToBool(creditCard)) set += 19
      if (intToBool(securities)) set += 20
      if (intToBool(homeAcc)) set += 21
      if (intToBool(payrollNom)) set += 22
      if (intToBool(pensionNom)) set += 23
      if (intToBool(directDebit)) set += 24

    var prod:String = set.mkString("[", ",", "]")
    Product(prod)
  }
}