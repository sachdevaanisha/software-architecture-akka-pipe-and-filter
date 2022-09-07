package assignment1

import akka.{Done, NotUsed}
import akka.actor.{ActorSystem, actorRef2Scala}
import akka.stream.{ActorMaterializer, Graph, IOResult, OverflowStrategy, _}
import akka.stream.scaladsl.{Balance,Broadcast, Compression, FileIO, Flow, Framing, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source, Zip, ZipWith2}
import akka.util.ByteString

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.{existentials, postfixOps}

object Assignment1 extends App{
  implicit val actorSystem: ActorSystem = ActorSystem("Assignment1")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = materializer.executionContext

  val path = Paths.get("src/main/resources/packages.txt.gz")
  val sourcePath: Source[ByteString, Future[IOResult]] = FileIO.fromPath(path)

  //Decompress the GZIP file using the Flow operator, Compression.gunzip()
  val flowUnzip: Flow[ByteString, ByteString, NotUsed] = Compression.gunzip()
  //To split the one long ByteString representing the whole file into 1 ByteString per newline
  val flowSplitPerLine: Flow[ByteString, ByteString, NotUsed] = Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024)
  //Convert the ByteStrings into Strings
  val flowStringCase: Flow[ByteString, String, NotUsed] = Flow[ByteString].map(_.utf8String)

  val composedFlow: Flow[ByteString, String, NotUsed] = flowUnzip.via(flowSplitPerLine).via(flowStringCase)
  //Flow which receives a string (package name) and emits one strings (package name) per 3 seconds using throttle
  // along with buffer size as 10 and overflow strategy as backpressure
  val bufferedSource: Flow[String, String, NotUsed] = Flow[String].buffer(10, OverflowStrategy.backpressure).throttle(1, 3 seconds)

  //To emit out the list of DependenciesPerVersion object for one package
  val flowVersion: Flow[String, List[DependenciesPerVersion], NotUsed] = {
    Flow[String].map(m => get_jsonData(m))
  }

  val flatteningFlow: Flow[List[DependenciesPerVersion], DependenciesPerVersion, NotUsed] =
    Flow[List[DependenciesPerVersion]].flatMapConcat(Source(_))

  //To control all versions we buffer 12 versions as maximum with backpressure strategy
  val versionBuffer = Flow[DependenciesPerVersion].buffer(12, OverflowStrategy.backpressure)

  val finalGraph: Graph[FlowShape[DependenciesPerVersion,TotalCountPerVersion], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val balance = builder.add(Balance[DependenciesPerVersion](2))
      val merge = builder.add(Merge[TotalCountPerVersion](2))

      val newFlow: Flow[DependenciesPerVersion, TotalCountPerVersion, NotUsed] =
        Flow[DependenciesPerVersion]
          .map(i =>
            TotalCountPerVersion.apply(i.packageName,i.versionNumber, i.dependenciesList.length, i.devDependenciesList.length, i.keywordsList))

      balance ~> newFlow ~> merge.in(0)
      balance ~> newFlow ~> merge.in(1)

      FlowShape(balance.in, merge.out)
    })

  val keywordGraph: Graph[FlowShape[TotalCountPerVersion,DisplayKeywords], NotUsed] = Flow.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[TotalCountPerVersion](2))
      val zippedValues = builder.add(new ZipWith2[TotalCountPerVersion, TotalCountPerVersion, DisplayKeywords]
      (
        (previousVersion, currentVersion) => {

          val previousPackageName = previousVersion.packageName
          val currentPackageName = currentVersion.packageName

          // As this project is an extension of session 1's project, earlier we were simply printing out
          // the names of analysing package in our get_json method but now we needed to separate the
          // streams of different packages so that the list of keywords could be differentiated for different packages
          // and the comparison of keywords list should just be between the versions of a same package.
          // As an extended logic, now we simply check if the two package names are different, we give an empty list of
          // strings as first keywords list for a new package.

          val previousKeywordsList = if(previousPackageName == currentPackageName) previousVersion.keywordList else List[String]()
          val currentKeywordsList = currentVersion.keywordList

          val previousKeywordsSet = previousKeywordsList.toSet
          val currentKeywordsSet = currentKeywordsList.toSet

          val additionalKeywords = currentKeywordsList.filterNot(previousKeywordsSet)
          val subtractedKeywords = previousKeywordsList.filterNot(currentKeywordsSet)

          DisplayKeywords(currentVersion.versionNumber, currentVersion.dependenciesTotalCount, currentVersion.devDependenciesTotalCount, additionalKeywords, subtractedKeywords)
        }
      ))

      val prependFlow: Flow[TotalCountPerVersion, TotalCountPerVersion, NotUsed] =
        Flow[TotalCountPerVersion].prepend(Source.single(TotalCountPerVersion("", "0.0.0", 0, 0, List[String]())))

      broadcast ~> prependFlow ~> zippedValues.in0
      broadcast ~> zippedValues.in1

      FlowShape(broadcast.in, zippedValues.out)
    })

  val sink: Sink[DisplayKeywords, Future[Done]] = Sink.foreach(println)

  val runnableGraph: RunnableGraph[Future[Done]] = sourcePath
    .via(composedFlow)
    .via(bufferedSource)
    .via(flowVersion)
    .via(flatteningFlow)
    .via(versionBuffer)
    .via(finalGraph)
    .via(keywordGraph)
    .toMat(sink)(Keep.right)

  runnableGraph.run().foreach(_ => actorSystem.terminate())

  //get_json method takes the name of the package and request the NPM registery API to get the information regarding
  // versions, dependencies lists and keywords lists of a particular package
  def get_jsonData(packageName: String): List[DependenciesPerVersion] = {
    val packageDetailList = ListBuffer[DependenciesPerVersion]()
    val url = s"https://registry.npmjs.org/$packageName"
    println(s"Analysing $packageName")
    val response = requests.get(url)
    val json = ujson.read(response.text)
    val versions = json.obj("versions")
    versions.obj.foreach(versionDetail => {
      val versionData = versionDetail._2
      val dependenciesVersion = versionData.obj.get("dependencies")
      val devDependenciesVersion = versionData.obj.get("devDependencies")
      val keyword = versionData.obj.get("keywords")
      val dependenciesSubList = ListBuffer[String]()
      val devDependenciesSubList = ListBuffer[String]()
      val keywordsSubList = ListBuffer[String]()

      if (dependenciesVersion.isDefined) dependenciesVersion.get.obj.keys.foreach(
        element => {
          dependenciesSubList += element
        }
      )

      if (devDependenciesVersion.isDefined) devDependenciesVersion.get.obj.keys.foreach(
        element => {
          devDependenciesSubList += element
        }
      )

      if(keyword.isDefined) {
        val a = keyword.get.arr
        val b = a.toList.foreach(
          elem => {
            val c = elem.str
            keywordsSubList += c
          }
        )
      }
      val dependenciesObject = DependenciesPerVersion(packageName,versionDetail._1, dependenciesSubList.toList, devDependenciesSubList.toList, keywordsSubList.toList)
      packageDetailList += dependenciesObject
    })

    packageDetailList.toList

  }

}

