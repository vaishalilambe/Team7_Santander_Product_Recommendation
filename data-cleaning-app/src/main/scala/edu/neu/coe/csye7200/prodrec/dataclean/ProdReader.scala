/** *****************************************************************
  * Team 7: Santander Product Recommendation
  * Date : 03/30/2018
  * **************************************************************/

package edu.neu.coe.csye7200.prodrec.dataclean


case class ProdReader(custCode:Int,date:String, savingAcc:Boolean, gaurantees:Boolean, currentAcc:Boolean, derivedAcc:Boolean, payrollAcc:Boolean, juniorAcc:Boolean, moreParticularAcc:Boolean, particularAcc:Boolean, particularPlusAcc:Boolean, shortTermDeposit:Boolean, midTermDeposit:Boolean, longTermDeposit:Boolean, eAccount:Boolean, funds:Boolean, mortgage:Boolean, pensionPlan:Boolean, loan:Boolean, taxes:Boolean, creditCard:Boolean, securities:Boolean, homeAcc:Boolean, payrollNom:Boolean, pensionNom:Boolean, directDebit:Boolean){
  override def toString: String = s"$custCode,$date,$savingAcc,$gaurantees,$currentAcc,$derivedAcc,$payrollAcc,$juniorAcc,$moreParticularAcc,$particularAcc,$particularPlusAcc,$shortTermDeposit,$midTermDeposit,$longTermDeposit,$eAccount,$funds,$mortgage,$pensionPlan,$loan,$taxes,$creditCard,$securities,$homeAcc,$payrollNom,$pensionNom,$directDebit"
}

object ProdReader extends App{

  def intToBool(number: Int):Boolean = number match {
    case 1 => true
    case 0 => false
    case _ => throw new Exception()
  }

  def apply(splitRow:Seq[String]):ProdReader = {

    val custCode = splitRow(1).replace(" ","").toDouble.toInt
    val date = splitRow.head.replace(" ","")

    val savingAcc = intToBool (splitRow(24).replace(" ","").toDouble.toInt)
    val gaurantees = intToBool(splitRow(25).replace(" ","").toDouble.toInt)
    val currentAcc = intToBool(splitRow(26).replace(" ","").toDouble.toInt)
    val derivedAcc = intToBool (splitRow(27).replace(" ","").toDouble.toInt)
    val payrollAcc = intToBool (splitRow(28).replace(" ","").toDouble.toInt)
    val juniorAcc = intToBool (splitRow(29).replace(" ","").toDouble.toInt)
    val moreParticularAcc = intToBool (splitRow(30).replace(" ","").toDouble.toInt)
    val particularAcc = intToBool (splitRow(31).replace(" ","").toDouble.toInt)
    val particularPlusAcc = intToBool (splitRow(32).replace(" ","").toDouble.toInt)
    val shortTermDeposit = intToBool (splitRow(33).replace(" ","").toDouble.toInt)
    val midTermDeposit = intToBool (splitRow(34).replace(" ","").toDouble.toInt)
    val longTermDeposit = intToBool (splitRow(35).replace(" ","").toDouble.toInt)
    val eAccount = intToBool (splitRow(36).replace(" ","").toDouble.toInt)
    val funds = intToBool (splitRow(37).replace(" ","").toDouble.toInt)
    val mortgage = intToBool (splitRow(38).replace(" ","").toDouble.toInt)
    val pensionPlan = intToBool (splitRow(39).replace(" ","").toDouble.toInt)
    val loan = intToBool (splitRow(40).replace(" ","").toDouble.toInt)
    val taxes = intToBool (splitRow(41).replace(" ","").toDouble.toInt)
    val creditCard = intToBool (splitRow(42).replace(" ","").toDouble.toInt)
    val securities = intToBool (splitRow(43).replace(" ","").toDouble.toInt)
    val homeAcc = intToBool(splitRow(44).replace(" ","").toDouble.toInt)
    val payrollNom = intToBool(splitRow(45).replace(" ","").toDouble.toInt)
    val pensionNom = intToBool(splitRow(46).replace(" ","").toDouble.toInt)
    val directDebit = intToBool(splitRow(47).replace(" ","").toDouble.toInt)

    ProdReader(custCode, date, savingAcc,gaurantees,currentAcc,derivedAcc,payrollAcc,juniorAcc,moreParticularAcc,particularAcc,particularPlusAcc,shortTermDeposit,midTermDeposit,longTermDeposit,eAccount,funds,mortgage,pensionPlan,loan,taxes,creditCard,securities,homeAcc,payrollNom,pensionNom,directDebit)

  }
}


