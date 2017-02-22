import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.LazyLogging


object SocrataServant extends App with LazyLogging with JsonWorkHorse {

  args.length match {
    case 0 => logger.error("No column field name was detected. Please rerun application with a valid Socrata column field name")
    case 1 => fetchData(args(0))
    case _ => logger.error("Multiple arguments were detected. Please rerun applicaiton with one argument")
  }

  def fetchData(str:String) = {
    val param = SocrataParams(str.toLowerCase.trim)
    val req = MetaDataExplorer.sendRequest(param)
    val md = MetaDataExplorer.getStringMetaData(req.body) // md = metadata
    val vdt = MetaDataExplorer.checkCol(md,param.colFieldName) //vdt = vector DatasetTools
    logger.info(s"Servant found ${vdt.size} potential datasets that contain the column ${param.colFieldName}")
    val fdt =  vdt.filter(_.url.isDefined) // fdt = filtered Vector DataSetTools
    val sd = fdt.map( dt => DatasetExplorer.getDataWithCol(dt)) //sd = source data
    val fsd = sd.filter(_.isDefined) // fsd = filtered source data
    logger.info(s"Servant got data from ${fsd.size} datasets")
    val output = jsonify(fsd)
    //output.take(2).foreach(println)
    saveToS3(output,param.colFieldName)
    output
  }

  def writeToFile(l:Vector[String],cfn:String) = {
    val pw = new PrintWriter(new File(s"${cfn}_socrata.json" ))
    l.foreach(s => pw.write(s + "\n"))
    pw.close()
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
