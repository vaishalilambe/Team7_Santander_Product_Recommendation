package edu.neu.coe.csye7200.prodrec.dataclean.model

import edu.neu.coe.csye7200.prodrec.dataclean.model.{Customer,Account, Product}


case class SantanderRecord(customerInfo: Customer, accountInfo: Account, productInfo: Product)
