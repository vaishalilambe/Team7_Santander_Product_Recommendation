package edu.neu.coe.csye7200.prodrec.dataclean
import scala.io.Source
/**
  * Trait responsible for reading/loading [[Customer]].
  *
  * Team 7 | Project-Santander product Recommendation 
  */
trait CustReader {

  /**
    * @return A [[Seq]] containing all the customers information/history.
    */
  def readCust(): Seq[Customer]

}

case class Customer(fecha_dato: String, ncodpers: String, ind_empleado:String, pais_residencia:String, sexo: String, age:String, fecha_alta:String,
                   ind_nuevo:String, antiguedad:String, indrel:String, ult_fec_cli_1t:String, indrel_1mes:String,tiprel_1mes:String,indresi:String,
                   indext:String, conyuemp:String, canal_entrada:String, indfall:String, tipodom:String, cod_prov:String,
                   nomprov:String, ind_actividad_cliente:String,renta:String,segmento:String)



/**
  * Implementation of [[CustReader]] responsible for reading sales from a CSV file.
  *
  * @param fileName The name of the CSV file to be read.
  * 
  */
class CustCSVReader(val fileName: String) extends CustReader {

  override def readCust(): Seq[Customer] = {
    for {
      line <- Source.fromFile(fileName).getLines().drop(1).toVector
      values = line.split(",").map(_.trim)
    } yield Customer(values(0), values(1), values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9),values(10),
      values(11),values(12),values(13),values(14),values(15),values(16),values(17),values(18),values(19),
      values(20),values(21),values(22),values(23))

  }

}