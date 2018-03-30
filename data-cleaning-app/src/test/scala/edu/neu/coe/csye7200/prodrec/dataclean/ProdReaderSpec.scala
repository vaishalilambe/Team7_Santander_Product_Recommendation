package edu.neu.coe.csye7200.prodrec.dataclean

import org.scalatest.{FlatSpec, Matchers}

class ProdReaderSpec extends FlatSpec with Matchers {
  behavior of "ProdReader.intToBool"
  it should "work for value 1 and return true" in {
    assert(ProdReader.intToBool(1) == true)
  }
}
