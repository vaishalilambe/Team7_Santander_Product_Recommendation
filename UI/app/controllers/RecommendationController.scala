package controllers

import javax.inject._
import play.api.mvc._
import scala.io.Source
import com.github.tototoshi.csv._

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
    Ok(views.html.recommendation("Recommendation application is ready."))
  }

  case class Prediction(customer_code:Int, products: Seq[Int])

  def getPredictions(id:Int) = Action {
    val sources = Source.fromFile("/tmp/trim_train.csv")
    Ok(views.html.predictions(sources))
  }
}
