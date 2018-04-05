package edu.neu.coe.csye7200.prodrec.dataclean.model

import scala.util.Try

case class Customer(
        code: Double,
        employmentStatus: String,
        countryOfResidence: String,
        gender: String,
        age: Option[Int],
        income: Option[Double]
        ) {
            override def toString: String = s"$code,$employmentStatus,$countryOfResidence,$gender,$age,$income"
        }

object Customer {
  def apply(
             code: String,
             employmentStatus: String,
             countryOfResidence: String,
             gender: String,
             age: String,
             income: String
           ): Customer = {

    val parsedCode: Double = code.trim.toDouble
    val parsedEStatus = employmentStatus.trim
    val parsedCResidency = countryOfResidence.trim
    val parsedGender = gender.trim
    val parsedAge = Try(age.trim.toInt).toOption
    val parsedIncome = Try(income.trim.toDouble).toOption

    new Customer(parsedCode, parsedEStatus, parsedCResidency, parsedGender, parsedAge, parsedIncome)
  }
}