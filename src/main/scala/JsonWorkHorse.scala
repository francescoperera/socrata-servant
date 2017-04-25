import io.circe.{Json, parser}

trait JsonWorkHorse {
  /**
    * converts String to Json type
    * @param str - Input String
    * @return Json version of str.
    */
  def toJson(str: String): Json = parser.parse(str).fold(_ => ???, json => json)

  def stringifyList(l:List[Json]): Vector[String] = l.head.asArray.get.map(_.asString.get)
}