import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer


object SocrataServant extends App with LazyLogging with JsonWorkHorse {

  private val limit = 10000
  private var offset = 0
  private val maxOffset = 20000

  args.length match {
    case 0 => logger.error("No column field name was detected. Please rerun application with a valid Socrata column field name")
    case 1 => fetchData(args(0))
    case _ => logger.error("Multiple arguments were detected. Please rerun applicaiton with one argument")
  }

  def fetchData(str:String) = {
    val vdt = ListBuffer[Vector[DatasetHttpParams]]()
    while (offset <= maxOffset){
      vdt += fetchDatasetTools(str,limit,offset)
      offset += limit
    }
    val fvdt: Vector[DatasetHttpParams] = vdt.toVector.flatten
    logger.info(s"Servant found ${fvdt.size} potential datasets that contain the column ${str}")
    val fdt:Vector[DatasetHttpParams] =  fvdt.filter(_.url.isDefined) // fdt = filtered Vector DataSetTools
    val sd : Vector[Option[String]] = fdt.map( dt => DatasetExplorer.getDataWithCol(dt)) //sd = source data
    val fsd = sd.filter(_.isDefined) // fsd = filtered source data
    logger.info(s"Servant got data from ${fsd.size} datasets")
    val output:Vector[Vector[String]] = unwrapVector(fsd)
    saveToS3(output,str)
    output
  }

  def fetchDatasetTools(str:String,lim:Int,offset:Int):Vector[DatasetHttpParams] = {
    val param = SocrataHttpParams(str.toLowerCase.trim,limit,offset)
    val req = MetaDataExplorer.sendRequest(param)
    val md = MetaDataExplorer.getStringMetaData(req.body) // md = metadata
    MetaDataExplorer.checkCol(md,param.colFieldName) //vdt = vector DatasetTools
  }

  /**
    * converts a Vector of stringified Json into a Vector of Vectors of Strings ( where the string represents a Json object_
    * @param v - vector of (Optional) String
    * @return - Vector of Vectors of Strings
    */
  def unwrapVector(v: Vector[Option[String]]): Vector[Vector[String]] = v.map(data => toJson(data.get).asArray.get.map(_.noSpaces))

  def saveToS3(l:Vector[Vector[String]],cfn:String) = {
    val f = new File(s"${cfn}_socrata.json" )
    val pw = new PrintWriter(f)
    l.flatten.foreach(s => pw.write( s + "\n"))
    pw.close()
    S3Client.saveFile(f)
    logger.info(s"saved ${f.getName} to S3, size : ${f.length()/1000} KB")
  }
}
