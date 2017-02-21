import com.typesafe.scalalogging.LazyLogging


object SocrataServant extends App with LazyLogging with JsonWorkHorse {

  args.length match {
    case 0 => logger.error("No column field name was detected. Please rerun application with a valid Socrata column field name")
    case 1 => fetchData(args(0))
    case _ => logger.error("Multiple arguments were detected. Please rerun applicaiton with one argument")
  }


  def fetchData(str:String):Vector[Vector[String]] = { //:
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
    println(output.take(5))
    output
  }

  //TODO: Follow SocrataExplorer code and write to file and push to S3
  //TODO: Look at Quip and gather data for Bloomberg classes
  //TODO: Rewrite this in Rust 

}
