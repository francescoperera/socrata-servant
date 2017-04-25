import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.syntax._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable



object SocrataServant extends LazyLogging with  JsonWorkHorse {

  private val limit = 10 //10000
  private var offset = 0
  private val maxOffset = 10//20000

  def main(args:Array[String]) = {
    args.length match {
      case 0 => logger.error("No column field name was detected. Please rerun application with a valid Socrata column field name")
      case 1 => fetchData(args(0))
      case _ => logger.error("Multiple arguments were detected. Please rerun applicaiton with one argument")
    }
  }

  /**
    * fetchData takes an input String, paginates through Socrata MetaData and uses an HTTP request to get data( that have the
    * input string as a column ) through all Socrata datasets that have the input String listed under column_field_name.
    * The method finally saves all data as new-line delimited json to a Datalogue S3 bucket.
    *
    * @param str - string
    */
  def fetchData(str:String) = {
    val ldhp = ListBuffer[Vector[DatasetParams]]()
    while (offset <= maxOffset){
      ldhp += fetchDatasetTools(str,offset)
      offset += limit
    }
    val fvhp: Vector[DatasetParams] = ldhp.toVector.flatten // fvhp = filtered vector of DatasetHtpParams
    logger.info(s"Servant found ${fvhp.size} potential datasets that contain the column ${str}")
    val fdt:Vector[DatasetParams] =  fvhp.filter(_.url.isDefined) // fdt = filtered Vector DatasetHttpParams
//    val sd : Vector[NDJSONParams] = fdt.map( dt => DatasetExplorer.getDataWithCol(dt)) //sd = source data
//    val fsd = sd.filter(_.data.isDefined) // fsd = filtered source data
//    logger.info(s"Servant got data from ${fsd.size} datasets")
//    val output:Vector[Vector[String]] = unwrapVector(fsd)
//    saveToS3(output,str)
  }

  /**
    * Takes an input string, limit and offset values to create a SocrataHTTParams, send an HTTP request. The method reads the
    * response and creates a Vector of DatasetHttpParams that will contain the url and column needed to then obtain the actual
    * data.
    *
    * @param str - input string ( i.e column field nam)
    * @param offset - Int ( This needed to paginate through Socrata)
    * @return - Vector of DatasetHttpParams(url,column)
    */
  def fetchDatasetTools(str:String,offset:Int):Vector[DatasetParams] = {
    val param = SocrataHttpParams(str.toLowerCase.trim,limit,offset)
    val req = MetaDataExplorer.sendRequest(param)
    val md = MetaDataExplorer.getStringMetaData(req.body) // md = metadata
    MetaDataExplorer.checkCol(md,param.colFieldName) //vdt = vector DatasetParams
  }

  /**
    * converts a Vector of stringified Json into a Vector of Vectors of Strings ( where the string represents a Json object)
    *
    * @param v - vector of (Optional) String
    * @return - Vector of Vectors of Strings
    */
  def unwrapVector(v: Vector[NDJSONParams]): Vector[Vector[String]] = {
    //TODO:Hacky. Rewrite
    val md = v.map { params => mappifyData(params.data,params.colDesc)}
    val jv = md.map( v => v.asJson)
    jv.map(v => v.asArray.get.map(_.noSpaces))


      //toJson(data.get).asArray.get.map(_.noSpaces)}
    //Vector(Vector(""))
  }

  def mappifyData(d:Option[String],colDesc:Option[String]) = { // : Vector[Map[String,String]]
    //TODO:Hacky solution. Rewrite
    val objs: Vector[Json] = toJson(d.get).asArray.get
    val vm : Vector[Map[String,Json]] = objs.map(_.asObject.get.toMap) //vm = vector of maps
    val newVM = vm.map(m => m.map{case(k,v) => (k,v.asString.getOrElse(""))})
    val mutMap  = newVM.map{ m => collection.mutable.Map(m.toSeq: _*)}
    val fm = mutMap.map{m => m += ("column_description" -> colDesc.getOrElse(""))}
    val immMap = fm.map(_.toMap)
    //val jm = immMap.map(m =>m.map{case(k,v) => (k.asJson,v.asJson)})
    immMap

  }

  def saveToS3(l:Vector[Vector[String]],cfn:String) = {
    val f = new File(s"${cfn}_socrata.json" )
    val pw = new PrintWriter(f)
    l.flatten.foreach(s => pw.write( s + "\n"))
    pw.close()
    S3Client.saveFile(f)
    logger.info(s"saved ${f.getName} to S3, size : ${f.length()/1000} KB")
  }
}
