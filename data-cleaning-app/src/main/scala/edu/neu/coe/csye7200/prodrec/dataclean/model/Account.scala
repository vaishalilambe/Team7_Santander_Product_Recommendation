package edu.neu.coe.csye7200.prodrec.dataclean.model

import java.sql.Date
import scala.util.Try

case class Account(
                    customerType: Option[String],
                    joinDate: Option[Date],
                    isCustomerAtMost6MonthOld: Option[Int],
                    seniority: Option[Int],
                    isPrimaryCustomer: Option[Int],
                    customerTypeFirstMonth: Option[String],
                    customerRelationTypeFirstMonth: Option[String],
                    customerResidenceIndex: Option[String],
                    customerForeignIndex: Option[String],
                    channelOfJoin: Option[String],
                    deceasedIndex: Option[String],
                    customerAddrProvinceName: Option[String],
                    isCustomerActive: Option[Int]
                  )

object Account {
  def apply(
             customerType: String,
             joinDate: String,
             isCustomerAtMost6MonthOld: String,
             seniority: String,
             isPrimaryCustomer: String,
             customerTypeFirstMonth: String,
             customerRelationTypeFirstMonth: String,
             customerResidenceIndex: String,
             customerForeignIndex: String,
             channelOfJoin: String,
             deceasedIndex: String,
             customerAddrProvinceName: String,
             isCustomerActive: String
           ): Account = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val parsedCustomerType = Try(customerType.split("-")(1).trim).toOption
    val parsedJoinDate = Try(new java.sql.Date(format.parse(joinDate.trim).getTime)).toOption
    val parsedIsCustomerAtMost6MonthOld = Try(isCustomerAtMost6MonthOld.trim.toInt).toOption
    val parsedSeniority = Try(seniority.trim.toInt).toOption
    val parsedIsPrimaryCustomer = Try(isPrimaryCustomer.trim.toInt).toOption
    val parsedCustomerTypeFirstMonth = Try(customerTypeFirstMonth.trim).toOption
    val parsedCustomerRelationTypeFirstMonth = Try(customerRelationTypeFirstMonth.trim).toOption
    val parsedCustomerResidenceIndex = Try(customerResidenceIndex.trim).toOption
    val parsedCustomerForeignIndex = Try(customerForeignIndex.trim).toOption
    val parsedChannelOfJoin = Try(channelOfJoin.trim).toOption
    val parsedDeceasedIndex = Try(deceasedIndex.trim).toOption
    val parsedCustomerAddrProvinceName = Try(customerAddrProvinceName.trim).toOption
    val parsedIsCustomerActive = Try(isCustomerActive.trim.toInt).toOption

    new Account(parsedCustomerType, parsedJoinDate, parsedIsCustomerAtMost6MonthOld, parsedSeniority, parsedIsPrimaryCustomer,
      parsedCustomerTypeFirstMonth, parsedCustomerRelationTypeFirstMonth, parsedCustomerResidenceIndex, parsedCustomerForeignIndex,
      parsedChannelOfJoin, parsedDeceasedIndex, parsedCustomerAddrProvinceName, parsedIsCustomerActive)
  }

}