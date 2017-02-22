import io.circe.{Json, parser}

trait JsonWorkHorse {

  /**
    * converts String to Json type
    *
    * @param str - Input String
    * @return Json version of str.
    */
  def toJson(str: String): Json = parser.parse(str).fold(_ => ???, json => json)

  //TODO:Rewrite doc strings for this
  /**
    * converts a Vector of stringified Json into an Array of Arrays of Strings ( where the string represents a Json object_
    *
    * @param v - vector of (Optional) String
    * @return - Array of Array of Strings
    */
  def jsonify(v: Vector[Option[String]]): Vector[Vector[String]] = v.map(data => toJson(data.get).asArray.get.map(_.noSpaces))

}