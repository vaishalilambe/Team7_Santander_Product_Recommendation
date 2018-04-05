/** *****************************************************************
  * Team 7: Santander Product Recommendation
  * Date : 03/30/2018
  * **************************************************************/

package edu.neu.coe.csye7200.prodrec.dataclean.model

case class Product(
                    savingAcc: Boolean,
                    gaurantees: Boolean,
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

  override def toString: String = s"$savingAcc,$gaurantees,$currentAcc,$derivedAcc,$payrollAcc,$juniorAcc,$moreParticularAcc," +
    s"$particularAcc,$particularPlusAcc,$shortTermDeposit,$midTermDeposit,$longTermDeposit,$eAccount,$funds,$mortgage," +
    s"$pensionPlan,$loan,$taxes,$creditCard,$securities,$homeAcc,$payrollNom,$pensionNom,$directDebit"
}

object Product {

  def intToBool(number: Int): Boolean = number match {
    case 1 => true
    case 0 => false
    case _ => throw new Exception()
  }

  def apply(
             savingAcc: String,
             gaurantees: String,
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

    val parsedSavingAcc = intToBool(savingAcc.trim.toDouble.toInt)
    val parsedGuarantees = intToBool(gaurantees.trim.toDouble.toInt)
    val parsedCurrentAcc = intToBool(currentAcc.trim.toDouble.toInt)
    val parsedDerivedAcc = intToBool(derivedAcc.trim.toDouble.toInt)
    val parsedPayrollAcc = intToBool(payrollAcc.trim.toDouble.toInt)
    val parsedJuniorAcc = intToBool(juniorAcc.trim.toDouble.toInt)
    val parsedMoreParticularAcc = intToBool(moreParticularAcc.trim.toDouble.toInt)
    val parsedParticularAcc = intToBool(particularAcc.trim.toDouble.toInt)
    val parsedParticularPlusAcc = intToBool(particularPlusAcc.trim.toDouble.toInt)
    val parsedShortTermDeposit = intToBool(shortTermDeposit.trim.toDouble.toInt)
    val parsedMidTermDeposit = intToBool(midTermDeposit.trim.toDouble.toInt)
    val parsedLongTermDeposit = intToBool(longTermDeposit.trim.toDouble.toInt)
    val parsedEAccount = intToBool(eAccount.trim.toDouble.toInt)
    val parsedFunds = intToBool(funds.trim.toDouble.toInt)
    val parsedMortgage = intToBool(mortgage.trim.toDouble.toInt)
    val parsedPensionPlan = intToBool(pensionPlan.trim.toDouble.toInt)
    val parsedLoan = intToBool(loan.trim.toDouble.toInt)
    val parsedTaxes = intToBool(taxes.trim.toDouble.toInt)
    val parsedCreditCard = intToBool(creditCard.trim.toDouble.toInt)
    val parsedSecurities = intToBool(securities.trim.toDouble.toInt)
    val parsedHomeAcc = intToBool(homeAcc.trim.toDouble.toInt)
    val parsedPayrollNom = intToBool(payrollNom.trim.toDouble.toInt)
    val parsedPensionNom = intToBool(pensionNom.trim.toDouble.toInt)
    val parsedDirectDebit = intToBool(directDebit.trim.toDouble.toInt)

    Product(parsedSavingAcc, parsedGuarantees, parsedCurrentAcc, parsedDerivedAcc, parsedPayrollAcc, parsedJuniorAcc,
      parsedMoreParticularAcc, parsedParticularAcc, parsedParticularPlusAcc, parsedShortTermDeposit, parsedMidTermDeposit,
      parsedLongTermDeposit, parsedEAccount, parsedFunds, parsedMortgage, parsedPensionPlan, parsedLoan, parsedTaxes,
      parsedCreditCard, parsedSecurities, parsedHomeAcc, parsedPayrollNom, parsedPensionNom, parsedDirectDebit)

  }
}