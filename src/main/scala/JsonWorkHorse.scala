import io.circe.{Json, parser}

trait JsonWorkHorse {
  /**
    * converts String to Json type
    * @param str - Input String
    * @return Json version of str.
    */
  def toJson(str: String): Json = parser.parse(str).fold(_ => ???, json => json)

}