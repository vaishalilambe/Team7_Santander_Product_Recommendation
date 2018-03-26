
import org.scalatest.{FlatSpec, Matchers}


class CustomerSpec extends FlatSpec with Matchers {

  behavior of "customer"

  it should "work for Int" in {
    assert(1+1 == 2)
  }

}