/** *****************************************************************
  * Team 7: Santander Product Recommendation
  * Date : 03/25/2018
  * **************************************************************/

package edu.neu.coe.csye7200.prodrec.dataclean

import scala.util.Try

/**
  * Customer reader class has customer related attributes of a santander bank
  *
  * @param Cust_Code    Unique code for the Customer of the bank
  * @param Cust_Age     Age of a bank customer
  * @param Cust_Sex     Gender of a bank customer
  * @param Cust_Income  Income of a bank customer
  * @param Cust_Id      Customer identification as VIP, Individual or Student
  * @param Cust_Type    Type of a customer as Primary, Co-owner, Potential
  * @param Cust_Addr    Address of a customer
  * @param Cust_Profile Bank profile of a customer
  */


case class CustReader(Cust_Code: Int, Cust_Age: Int, Cust_Sex: String, Cust_Income: Double, Cust_Id: Int, Cust_Type: Int, Cust_Addr: Address, Cust_Profile: BankProfile)


/**
  * Address class has address related attributes for a customer of a santander bank
  *
  * @param Primary_Addr    Primary Address of a customer
  * @param Country         Country of a customer
  * @param Province_Name   Province Name of a customer
  * @param Province_Code   Province Code of a customer
  * @param Residence_Index Residence Index of a customer
  * @param Foreign_Index   Foreign Index of a customer
  */
case class Address(Primary_Addr: Boolean, Country: String, Province_Name: String, Province_Code: Int, Residence_Index: Boolean, Foreign_Index: Boolean) {
  override def toString: String = s"$Primary_Addr,$Country,$Province_Name ,$Province_Code ,$Residence_Index ,$Foreign_Index"
}

/**
  * BankProfile class has Bank related attributes for a customer of a bank
  *
  * @param New_Index        New Index of a customer
  * @param Join_Date        Joining Date of a Customer
  * @param Seniority        Seniority of a customer
  * @param Emp_Index        Employee Index of a customer
  * @param Emp_Spouse_Index Employee Spouse Index for a customers who are employee
  */
case class BankProfile(New_Index: Boolean, Join_Date: String, Seniority: Int, Emp_Index: Boolean, Emp_Spouse_Index: Boolean) {
  override def toString: String = s"$New_Index, $Join_Date, $Seniority, $Emp_Index, $Emp_Spouse_Index"
}


object Customer extends App {

  def apply(splitRow: Seq[String]): CustReader = {

    val Cust_Code = splitRow(1).replace(" ", "").toInt

    val Cust_Age = splitRow(5).replace(" ", "").toDouble.toInt

    val Cust_Sex = splitRow(4).replace(" ", "").toUpperCase

    val Cust_Income = splitRow(22).replace(" ", "").toDouble

    val Cust_Id = splitRow(23).split("-")(0).replace(" ", "").replace(""""""", "").toInt

    val Cust_Type = splitRow(11).replace(" ", "").toDouble.toInt

    val Cust_Addr = Cust_Addr(Seq(splitRow(18), splitRow(3), splitRow(20), splitRow(19), splitRow(13), splitRow(14)))

    val Cust_Profile = Cust_Profile(Seq(splitRow(7), splitRow(6), splitRow(8), splitRow(2), splitRow(15)))

    CustReader(Cust_Code, Cust_Age, Cust_Sex, Cust_Income, Cust_Id, Cust_Type, Cust_Addr, Cust_Profile)
  }

  trait IngestibleCustReader extends Ingestible[CustReader] {
    def fromString(row: String): Try[CustReader] = {
      // To-do use regex
      Try(Customer(row.split(""",(?=([^\"]*\"[^\"]*\")*[^\"]*$)""")))
    }

  }

}

object Address{
  def apply(params: Seq[String]): Address = {
    params match {
      case primaryAddress :: country :: provinceName :: provinceCode :: residenceIndex :: foreignIndex :: Nil=>
        apply( {if(primaryAddress.replace(" ","").toDouble.toInt == 1) true else false},
          country, provinceName,
          provinceCode.replace(" ", "").toInt,
          {residenceIndex.replace(" ","").toUpperCase() match {case "S" => true case _ => false}},
          {foreignIndex.replace(" ","").toUpperCase match  {case "S" => true case _ => false}})
      case _ => throw new Exception(s"parse error in Name: $this")
    }
  }
}

object BankProfile{
  def apply(params: Seq[String]): BankProfile = {
    params match {
      case newIndex :: joinDate :: seniority :: employeeIndex :: employeeSpouseIndex :: Nil =>
        apply(
          newIndex.replace(" ","") match {
            case "" => false
            case str => if(str.toDouble.toInt == 1) true else false
          },

          joinDate,

          {seniority.replace(" ","") match  {
            case "" => 0
            case str => str.toDouble.toInt
          }},

          employeeIndex.replace(" ", "") match {
            case "" => false
            case str => if(str.toUpperCase == "N") false else true
          },

          { employeeSpouseIndex.replace(" ", "") match {
            case "" => false
            case str =>  if (str.toDouble.toInt == 1) true else false
          }
          })

      case _ => throw new Exception(s"parse error in Name: $this")
    }
  }
}



