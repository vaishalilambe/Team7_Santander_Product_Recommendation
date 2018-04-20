package edu.neu.coe.csye7200.prodrec.dataclean.model

import scala.util.{Failure, Success, Try}


case class Customer(
        code: Option[Int],
        employmentStatus: String,
        countryOfResidence: String,
        gender: String,
        age: Option[Int],
        income: Option[Double]
        )

object Customer {
  def apply(
             code: String,
             employmentStatus: String,
             countryOfResidence: String,
             gender: String,
             age: String,
             income: String
           ): Customer = {

    val parsedCode = Try(code.trim.toInt).toOption
    val parsedEStatus = employmentStatus.trim
    val parsedCResidency = if (countryOfResidence.trim == "") "ES" else countryOfResidence.trim
    val parsedGender = gender.trim
    val parsedAge = Try(age.trim.toInt) match {
      case Success(x) => Some(x)
      case Failure(e) => None
    }

    val parsedIncome = Try(income.trim.toDouble).toOption

    new Customer(parsedCode, parsedEStatus, parsedCResidency, parsedGender, parsedAge, parsedIncome)
  }
}