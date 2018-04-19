package edu.neu.coe.csye7200.prodrec.dataclean

import edu.neu.coe.csye7200.prodrec.dataclean.model.{Account, Customer, Product}
import org.scalatest.{FlatSpec, Matchers}


class SantenderRecordSpec extends FlatSpec with Matchers {

  behavior of "Customer"

  it should "work for input" in {
    Customer("12345", "A","", "M", "34", "34000") should matchPattern {
      case Customer(Some(12345), "A", "ES", "M", Some(34), Some(34000)) =>
    }
    Customer("34567", "B", "China", "F", "", "50000") should matchPattern {
      case Customer(Some(34567), "B", "China", "F", None, Some(50000)) =>
    }
    Customer("34567", "B", "China", "F", "", "") should matchPattern {
      case Customer(Some(34567), "B", "China", "F", None, None) =>
    }
  }

  behavior of "Account"

  it should "work for input" in {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val sqlDate = new java.sql.Date(format.parse("2015-11-12").getTime)

    Account("01 - VIP", "2015-11-12", "1", "4", "1", "1", "A-Active", "S", "S", "channel", "N", "NA", "1") should matchPattern {
      case Account(Some("VIP"), sqlDate, Some(1), Some(4), Some(1), Some("1"), Some("A-Active"), Some("S"), Some("S"), Some("channel"), Some("N"), Some("NA"), Some(1)) =>
    }
    Account("01 - VIP", "2015-11", "1", "", "1", "1", "A-Active", "S", "S", "channel", "N", "NA", "2d1") should matchPattern {
      case Account(Some("VIP"), None, Some(1), None, Some(1), Some("1"), Some("A-Active"), Some("S"), Some("S"), Some("channel"), Some("N"), Some("NA"), None) =>
    }
    Account("01 - VIP", "201-12", "1d", "4", "", "1", "A-Active", "S", "S", "channel", "N", "NA", "1") should matchPattern {
      case Account(Some("VIP"), None, None, Some(4), None, Some("1"), Some("A-Active"), Some("S"), Some("S"), Some("channel"), Some("N"), Some("NA"), Some(1)) =>
    }
  }

  behavior of "Product"

  it should "work for input" in {
    Product("0","1","0","1","1","0","1","0","1","1","1","1","0","1","0","1","1","0","1","0","1","1","1","1") should matchPattern{
      case Product("[2,4,5,7,9,10,11,12,14,16,17,19,21,22,23,24]") =>
    }
    Product("0","1","0","1","1","0","1","0","1","1","1","1","0","1","0","1","1","0","1","0","1","1  ","   1","1") should matchPattern{
      case Product("[2,4,5,7,9,10,11,12,14,16,17,19,21,22,23,24]") =>
    }
  }

  behavior of "Product.intToBool"
  it should "work for valid input" in {
    Product.intToBool("1") shouldBe true
    Product.intToBool(" 1 ") shouldBe true
    Product.intToBool("0") shouldBe false
    Product.intToBool("0 ") shouldBe false
    Product.intToBool("") shouldBe false
    Product.intToBool("dfg") shouldBe false
  }

}