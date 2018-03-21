package edu.neu.coe.csye7200.prodrec.dataclean

import org.scalatest.{FlatSpec, Matchers}


class IngestSpec extends FlatSpec with Matchers {

}

import org.scalatest.FunSuite
import org.scalatest.Matchers._

/**
  *
  * Team7 | Project-Santander product Recommendation 
  */
class CustCSVReaderTest extends FunSuite {

  test("Load CSV file") {
    val customers = new CustCSVReader("src/main/resources/train_ver2.csv").readCust

    //customers.size shouldBe 0

    customers(0) shouldBe Customer("2015-01-28","1375586","N","ES","H", "35","2015-01-12", "0","6", "1","","1.0","A","S","N","","KHL","N", "1","29","MALAGA", "1",
      "87218.1","02 - PARTICULARES")

  }

}