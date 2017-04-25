import java.net.{SocketTimeoutException, UnknownHostException}
import javax.net.ssl.SSLHandshakeException

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

import scalaj.http.{Http, HttpOptions, HttpResponse}

case class SocrataHttpParams(colFieldName:String,limit:Int,offset:Int) /** contains all params need for a Socrata HTTP  Request */
case class MetaData(cfn:Vector[String],cd:Vector[String],pl:Option[String]) /** contains a list of column field name,list of column descriptions and permalink from metadata. */
case class DatasetParams(url:Option[String],colFieldName:Vector[String],colDesc:Vector[Option[String]]) /** contains all params needed to get dataset through HTTP */
case class NDJSONParams(data:Option[String],colDesc:Option[String]) /** params needed to create NDJSON objects. data  from column field name and corresponding column_description */

object MetaDataExplorer extends LazyLogging with JsonWorkHorse {

  private val token = "GPGuyRELzwEXtRJbJDib89U59"

  /**
    * Takes type SocrataParams, reads the columns and sends an http request to Socrata with the given cols.
    * @param sp - SocrataParams
    * @return Http Response in String format
    */
  def sendRequest(sp:SocrataHttpParams):HttpResponse[String] =
    Http(s"http://api.us.socrata.com/api/catalog/v1?only=datasets&q=${sp.colFieldName}&offset=${sp.offset}&limit=${sp.limit}")
      .header("X-App-Token",token).asString

  /**
    * Takes the body of a Http Response and returns the content under the key results
    * @param body - String
    * @return - optional vector of content under results
    */
  def getStringMetaData(body:String):Option[Vector[Json]] = toJson(body).asObject.get.apply("results").get.asArray

  /**
    * Takes metadata( optional vector of Json) and a column name. For each Json object, the function grabs the content under
    * columns_field_name and permalink keys and uses them to create a MetaData object.Then, for each MetaData object it checks whether the
    * input column is present in MetaData.cfn. If that is true, then the MetaData object is replaced by just MetaData.pl. If not true,
    * the object is replaced by  None
    * @param md - Vector of all metadata (Json) objects
    * @param col - column field name
    * @return - Vector of DatasetTools(url,column field name)
    */
  def checkCol(md:Option[Vector[Json]], col:String): Vector[DatasetParams]= {
    val fmd: Vector[MetaData] = md.get.flatMap{obj =>
      obj.asObject match {
        case None => None
        case Some(o) =>
          // TODO: loose JSON assumptions. Fix
          val resource: Json = o.apply("resource").get
          val cfn: Vector[String] = resource.asObject.get.apply("columns_field_name").get.
            asArray.get.flatMap(_.asString)
          val cd: Vector[String] = resource.asObject.get.apply("columns_description").get.
            asArray.get.flatMap(_.asString)
          val permalink : Option[String] = o.apply("permalink").get.asString
          println(cfn)
          println(cd)
          println(permalink)
          println()
          Some(MetaData(cfn,cd,permalink))
      }
    }
//      val cfn : Vector[String] = stringifyList(obj.\\("resource").map(_.\\("columns_field_name")).head)
//      val cd :Vector[String] = stringifyList(obj.\\("resource").map(_.\\("columns_description")).head)
//      val permalink: Option[String] = obj.\\("permalink").head.asString
//      MetaData(cfn,cd,permalink)}
    val vdp: Vector[DatasetParams] = fmd.map{ meta => meta.cfn.contains(col) match {
      case true =>
        val cdIdx : Int = meta.cfn.indexOf(col)
        val colDesc : String = meta.cd(cdIdx)
        ////////
        if( col == "first_name"){
          //check last_name is present
          val ln: String = "last_name"
          if (meta.cfn.contains(ln)){
            val lnIdx : Int = meta.cfn.indexOf("last_name") //ln = last_name
            val lnDesc : String = meta.cd(lnIdx)
            DatasetParams(meta.pl,Vector(col,ln),Vector(Some(colDesc),Some(lnDesc)))
          }
          else{
            DatasetParams(None,Vector(col),Vector(None))
          }
        }else{
          DatasetParams(meta.pl,Vector(col),Vector(Some(colDesc)))
        }
        ////////
      case false =>
        DatasetParams(None,Vector(col),Vector(None))
    }}
    println(vdp)
    vdp
  }
}

object DatasetExplorer extends LazyLogging{

  /**
    * Parse the permalink and substitute /d/ with /resource/
    * @param pl - permalink
    * @return - edited permalink with /resource/ in it
    */
  def parsePermaLink(pl:String):String = pl.split("/").toVector.updated(3,"resource").mkString("/")

  /**
    * sends an Http request with the specified column field name
    * @param dst - DatasetTools( contains dataset url and column field name)
    * @return - HttpResponse in String format
    */
//  def getDataWithCol(dst:DatasetParams):NDJSONParams = {
//    val ppl = parsePermaLink(dst.url.get) //parsed permalink = ppl
//    val select = "$select"
//    val url = s"$ppl.json?$select=${dst.colFieldName.toLowerCase}"
//    logger.info(s"sending HTTP request to $url")
//    try{
//      Thread.sleep(100)
//      val resp = Http(url).option(HttpOptions.readTimeout(50000)).option(HttpOptions.connTimeout(10000)).asString
//      logger.info(s"HTTP response code from $url is :${resp.code}")
//      resp.isNotError match {
//        case true => NDJSONParams(Some(resp.body),dst.colDesc)
//        case false => NDJSONParams(None,dst.colDesc)
//      }
//    }
//    catch {
//      case uhe: UnknownHostException => NDJSONParams(None,None)
//      case ste: SocketTimeoutException =>  NDJSONParams(None,None)
//      case ssl: SSLHandshakeException =>  NDJSONParams(None,None)
//    }
//  }
}
