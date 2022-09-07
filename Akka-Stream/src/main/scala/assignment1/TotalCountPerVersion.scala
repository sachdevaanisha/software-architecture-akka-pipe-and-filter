package assignment1

case class TotalCountPerVersion(
                                 packageName: String,
                                 versionNumber: String,
                                 dependenciesTotalCount: Int,
                                 devDependenciesTotalCount: Int,
                                 keywordList: List[String]
                               )
