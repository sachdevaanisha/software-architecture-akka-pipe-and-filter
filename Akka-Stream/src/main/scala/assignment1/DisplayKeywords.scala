package assignment1

case class DisplayKeywords(versionNumber: String,
                           dependenciesTotalCount: Int,
                           devDependenciesTotalCount: Int,
                           additionalKeywords: List[String],
                           subtractedKeywords: List[String]
                          ) {
  override def toString: String = {
    s"Version: $versionNumber, " +
      s"Dependencies: $dependenciesTotalCount, " +
      s"DevDependencies: $devDependenciesTotalCount" +
      { if(additionalKeywords.nonEmpty || subtractedKeywords.nonEmpty)
        s", Keywords: ${'"'}" +
          {if(additionalKeywords.nonEmpty && subtractedKeywords.nonEmpty)
            additionalKeywords.mkString("+", ", +","") + subtractedKeywords.mkString(", -", ", -", s"${'"'}")
          else
            if(additionalKeywords.nonEmpty)
              additionalKeywords.mkString("+", ", +", s"${'"'}")
            else
              subtractedKeywords.mkString("-", ", -", s"${'"'}")
          }
      else
        s" "}
  }

}