import java.util.Date
import scala.util.Try

case class Account(
                    customerType: Int,
                    joinDate: Option[Date],
                    isCustomerAtMost6MonthOld: Int,
                    seniority: Int,
                    isPrimaryCustomer: Int,
                    customerTypeFirstMonth: Option[String],
                    customerRelationTypeFirstMonth: Option[String],
                    customerResidenceIndex: Option[String],
                    customerForeignIndex: Option[String],
                    channelOfJoin: Option[String],
                    deceasedIndex: Option[String],
                    customerAddrProvinceName: String,
                    isCustomerActive: Int
                  ) {
  override def toString: String = s"$customerType,$joinDate,$isCustomerAtMost6MonthOld,$seniority,$isPrimaryCustomer," +
    s"$customerTypeFirstMonth,$customerRelationTypeFirstMonth,$customerResidenceIndex,$customerForeignIndex," +
    s"$channelOfJoin,$deceasedIndex,$customerAddrProvinceName,$isCustomerActive"

}

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

    val parsedCustomerType = customerType.trim.toInt
    val parsedJoinDate = Try(format.parse(joinDate.trim)).toOption
    val parsedIsCustomerAtMost6MonthOld = isCustomerAtMost6MonthOld.trim.toInt
    val parsedSeniority = seniority.trim.toInt
    val parsedIsPrimaryCustomer = isPrimaryCustomer.trim.toInt
    val parsedCustomerTypeFirstMonth = Try(customerTypeFirstMonth.trim).toOption
    val parsedCustomerRelationTypeFirstMonth = Try(customerRelationTypeFirstMonth.trim).toOption
    val parsedCustomerResidenceIndex = Try(customerResidenceIndex.trim).toOption
    val parsedCustomerForeignIndex = Try(customerForeignIndex.trim).toOption
    val parsedChannelOfJoin = Try(channelOfJoin.trim).toOption
    val parsedDeceasedIndex = Try(deceasedIndex.trim).toOption
    val parsedCustomerAddrProvinceName = customerAddrProvinceName.trim
    val parsedIsCustomerActive = isCustomerActive.trim.toInt

    new Account(parsedCustomerType, parsedJoinDate, parsedIsCustomerAtMost6MonthOld, parsedSeniority, parsedIsPrimaryCustomer,
      parsedCustomerTypeFirstMonth, parsedCustomerRelationTypeFirstMonth, parsedCustomerResidenceIndex, parsedCustomerForeignIndex,
      parsedChannelOfJoin, parsedDeceasedIndex, parsedCustomerAddrProvinceName, parsedIsCustomerActive)
  }

}