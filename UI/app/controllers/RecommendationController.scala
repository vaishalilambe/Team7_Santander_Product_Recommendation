package controllers

import javax.inject._

import scala.io.Source
import play.api.mvc._
import com.github.tototoshi.csv._
import scala.collection.mutable.TreeSet

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class RecommendationController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  /**
    * Create an Action to render an HTML page with a welcome message.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */
  def index = Action {
    Ok(views.html.recommendation("Ready for the Recommendation"))
  }

  def getPredictions(id:Int) = Action {
    val sources = Source.fromFile("dataset/predictionresult.csv")

    val prodMap = Map(
      "1" -> "Saving Account",
      "2" -> "Guarantees",
      "3" -> "Current Acc",
      "4"  -> "Derived Acc",
      "5"-> "Payroll Acc",
      "6"-> "Junior Acc",
      "7" -> "More Particular Acc",
      "8" -> "Particular Acc",
      "9" -> "Particular Plus Acc",
      "10" -> "Short Term Deposit",
      "11" -> "Mid Term Deposit",
      "12" -> "Long Term Deposit",
      "13" -> "eAccount",
      "14" -> "Funds",
      "15" -> "Mortgage",
      "16" -> "Pension Plan",
      "17" -> "Loan",
      "18" -> "Taxes",
      "19" -> "Credit Card",
      "20"->  "Securities",
      "21" -> "Home Acc",
      "22"-> "Payroll Nom",
      "23" -> "Pension Nom",
      "24" -> "Direct Debit"
    )
    val cId = id
    val set = TreeSet[String]()
    val regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    for(line <- sources.getLines())
      {
        val cid = line.split(regex)(0)
        val prodList = line.split(regex)(1)

        if (cid == cId.toString) {
          var myprodList = prodList.substring(2,prodList.length-2).split(",")
          for(x <- myprodList)
            {
              set.add(prodMap.get(x).get)
            }
        }
      }
    Ok(views.html.predictions(cId, set.toSeq))
  }
}