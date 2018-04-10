/** *****************************************************************
  * Team 7: Santander Product Recommendation
  * Date : 03/30/2018
  * **************************************************************/

package edu.neu.coe.csye7200.prodrec.dataclean.model

import scala.util.{Failure, Success, Try}

case class Product(
                    savingAcc: Boolean,
                    guarantees: Boolean,
                    currentAcc: Boolean,
                    derivedAcc: Boolean,
                    payrollAcc: Boolean,
                    juniorAcc: Boolean,
                    moreParticularAcc: Boolean,
                    particularAcc: Boolean,
                    particularPlusAcc: Boolean,
                    shortTermDeposit: Boolean,
                    midTermDeposit: Boolean,
                    longTermDeposit: Boolean,
                    eAccount: Boolean,
                    funds: Boolean,
                    mortgage: Boolean,
                    pensionPlan: Boolean,
                    loan: Boolean,
                    taxes: Boolean,
                    creditCard: Boolean,
                    securities: Boolean,
                    homeAcc: Boolean,
                    payrollNom: Boolean,
                    pensionNom: Boolean,
                    directDebit: Boolean) {

  override def toString: String = s"$savingAcc,$guarantees,$currentAcc,$derivedAcc,$payrollAcc,$juniorAcc,$moreParticularAcc," +
    s"$particularAcc,$particularPlusAcc,$shortTermDeposit,$midTermDeposit,$longTermDeposit,$eAccount,$funds,$mortgage," +
    s"$pensionPlan,$loan,$taxes,$creditCard,$securities,$homeAcc,$payrollNom,$pensionNom,$directDebit"
}

object Product {

  def intToBool(number: String): Boolean = Try(number.trim.toInt) match {
    case Success(1) => true
    case Success(0) => false
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

    val parsedSavingAcc = intToBool(savingAcc)
    val parsedGuarantees = intToBool(guarantees)
    val parsedCurrentAcc = intToBool(currentAcc)
    val parsedDerivedAcc = intToBool(derivedAcc)
    val parsedPayrollAcc = intToBool(payrollAcc)
    val parsedJuniorAcc = intToBool(juniorAcc)
    val parsedMoreParticularAcc = intToBool(moreParticularAcc)
    val parsedParticularAcc = intToBool(particularAcc)
    val parsedParticularPlusAcc = intToBool(particularPlusAcc)
    val parsedShortTermDeposit = intToBool(shortTermDeposit)
    val parsedMidTermDeposit = intToBool(midTermDeposit)
    val parsedLongTermDeposit = intToBool(longTermDeposit)
    val parsedEAccount = intToBool(eAccount)
    val parsedFunds = intToBool(funds)
    val parsedMortgage = intToBool(mortgage)
    val parsedPensionPlan = intToBool(pensionPlan)
    val parsedLoan = intToBool(loan)
    val parsedTaxes = intToBool(taxes)
    val parsedCreditCard = intToBool(creditCard)
    val parsedSecurities = intToBool(securities)
    val parsedHomeAcc = intToBool(homeAcc)
    val parsedPayrollNom = intToBool(payrollNom)
    val parsedPensionNom = intToBool(pensionNom)
    val parsedDirectDebit = intToBool(directDebit)

    Product(parsedSavingAcc, parsedGuarantees, parsedCurrentAcc, parsedDerivedAcc, parsedPayrollAcc, parsedJuniorAcc,
      parsedMoreParticularAcc, parsedParticularAcc, parsedParticularPlusAcc, parsedShortTermDeposit, parsedMidTermDeposit,
      parsedLongTermDeposit, parsedEAccount, parsedFunds, parsedMortgage, parsedPensionPlan, parsedLoan, parsedTaxes,
      parsedCreditCard, parsedSecurities, parsedHomeAcc, parsedPayrollNom, parsedPensionNom, parsedDirectDebit)

  }
}