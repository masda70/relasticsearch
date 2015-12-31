package com.masda70.relasticsearch

/**
  * @author David Montoya
  */


import akka.stream.scaladsl._
import org.scalatest._
import java.io.{ByteArrayOutputStream, InputStream, File}
import java.nio.charset.Charset

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.{Settings, ImmutableSettings}
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.node.Node
import org.elasticsearch.node.NodeBuilder._
import org.json4s.jackson.Serialization._
import futures.ListenableActionFutureExtensions._


import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * @author  David Montoya
  */
case class Literal(value: String)

class LocalElasticSearchTestServer private(node: Node, val indexName: String)(implicit executionContext: ExecutionContext)
  extends StrictLogging {

  val client = node.client()

  private def recreateIndex() = {
    deleteIndex().flatMap{
      case _ =>
        val utf8Charset = Charset.forName("UTF-8")
        val mappings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("mappings.json")
        val indexSettings: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("index_settings.json")
        client.admin.indices.prepareCreate(indexName).setSettings(IOUtils.toString(indexSettings, utf8Charset)).execute.future.flatMap{
          case _ => client.admin.indices.preparePutMapping(indexName).setType("literal").setSource(IOUtils.toString(mappings, utf8Charset)).execute.future
        }
    }.map{
      case _ => ()
    }
  }

  private def deleteIndex() = {
    client.admin.indices.prepareDelete(indexName).execute.future.map{
      case _ => ()
    }.recover{
      case e: IndexMissingException => ()
    }
  }

  def add(entities: Traversable[(String,String)]): Future[Unit] = {
    implicit val formats = org.json4s.DefaultFormats
    val bulkRequest = client.prepareBulk()
    entities.foreach {
      case (id, literal) =>
        bulkRequest.add(new IndexRequest(indexName).`type`("literal").id(id).source(write(Literal(literal))))
    }
    bulkRequest.execute.future.flatMap{
      case _ =>
        client.admin().indices().prepareRefresh(indexName).execute().future.map(_ => ())
    }
  }

  def close() = {
    deleteIndex().map{
      _ =>
        client.close()
        node.close()
        ()
    }
  }

}

object LocalElasticSearchTestServer extends StrictLogging{

  final val dataDirectory = "data"

  private def setupDirectories(dataDirectoryName: String, clusterDirectoryName: String) = {
    val esDirectory = new File(dataDirectoryName, clusterDirectoryName)
    val pluginDirectory: File = new File(esDirectory, "plugins")
    val scriptsDirectory: File = new File(esDirectory, "config/scripts")
    for (directory <- Array[File](esDirectory, pluginDirectory, scriptsDirectory)) {
      directory.mkdirs
    }
    esDirectory
  }

  def apply(clusterName: String, indexName: String, port: Int)(implicit executionContext: ExecutionContext) = {
    val host = "127.0.0.1"
    val sBuilder: ImmutableSettings.Builder = ImmutableSettings.settingsBuilder
    sBuilder.put("path.home", setupDirectories(dataDirectory, clusterName).toString)
    sBuilder.put("network.host", host)
    sBuilder.put("transport.tcp.port", port)
    val settings: Settings = sBuilder.build
    val esNode = nodeBuilder.clusterName(clusterName).settings(settings).node
    esNode.start()
    logger.info(s"[elastic-search] Started ES cluster $clusterName at $host:$port.")
    val server = new LocalElasticSearchTestServer(esNode, indexName)
    server.recreateIndex().map{
      _ => server
    }
  }


}


class IndexerTest extends FlatSpec with Matchers with StrictLogging {

  val text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus eu leo ex. Nunc vehicula nulla ut dolor malesuada, vel egestas ligula imperdiet. In hac habitasse platea dictumst. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed et justo molestie lacus iaculis tincidunt in at massa. Sed varius imperdiet congue. Curabitur pharetra eros sed posuere mattis.  Suspendisse ac erat feugiat, rhoncus justo ut, laoreet diam. Donec dictum est aliquam massa facilisis ornare. Quisque commodo quam ex, ac vulputate felis malesuada et. Sed in ante sed leo ultricies aliquam. Maecenas gravida massa in neque blandit gravida. Aenean leo mauris, eleifend a dui iaculis, ornare euismod nunc. Morbi ac pretium arcu. Donec in turpis vel ex luctus dictum. Sed a mi sit amet risus porta auctor. Pellentesque id dapibus dolor. Praesent et nisl consequat, pretium magna eget, placerat tellus. Maecenas efficitur magna augue, et aliquam turpis pharetra nec.  Suspendisse consectetur ut augue id suscipit. Nullam bibendum nulla vitae aliquam rutrum. Ut consectetur augue felis, eu luctus sem ullamcorper sit amet. Cras blandit accumsan eleifend. Aenean at placerat justo. Phasellus cursus velit quis turpis rhoncus, eu pharetra ligula molestie. Mauris condimentum, lacus at tempor bibendum, enim purus fermentum nibh, eu mollis diam lectus vel nulla. Proin vestibulum purus vitae nisi iaculis sollicitudin. Curabitur leo risus, egestas id ultricies vitae, ullamcorper ac dolor. Suspendisse ullamcorper augue at enim pretium, ut posuere tortor efficitur. Integer imperdiet felis non pretium imperdiet. Phasellus facilisis egestas ante, nec cursus dolor cursus id. Fusce lacus ex, aliquam et nulla sit amet, egestas accumsan odio. Cras in ligula sed neque fringilla sollicitudin in sed augue. Duis iaculis egestas tincidunt.  Nunc at sem at libero mollis semper. Fusce dictum ex enim, tristique imperdiet justo viverra a. Nam a pharetra lacus, sed bibendum purus. Vestibulum bibendum tincidunt risus sed euismod. Nam viverra commodo eros, nec accumsan metus luctus id. Proin tellus nulla, vestibulum non pretium posuere, posuere eu eros. Aliquam fermentum luctus nulla, sit amet eleifend velit blandit sit amet. Donec posuere nisi nisl, tincidunt dictum nisl cursus et. Praesent quis tortor pretium, bibendum est et, bibendum orci. Mauris et auctor massa. Cras finibus molestie nunc, a facilisis nulla laoreet non. Sed sit amet lacinia ante. Pellentesque sed lorem a ex fermentum ultricies eu semper lectus. Nullam eu tincidunt ante. Nunc eu rutrum lectus, non ornare felis.  Etiam tincidunt turpis id justo laoreet imperdiet. Aenean varius nulla in purus sodales egestas. Nunc malesuada a turpis id mattis. Vestibulum dignissim, sapien id accumsan iaculis, justo nulla finibus lectus, ac sagittis leo est eu arcu. Curabitur at tortor lorem. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed tincidunt ante in posuere malesuada. Nullam at quam eleifend, scelerisque nulla scelerisque, viverra lacus. Pellentesque in placerat mauris.  Praesent egestas lectus eu ipsum dictum, at auctor ante sodales. Praesent sagittis nisl tincidunt nisi ullamcorper, id fermentum elit finibus. Sed quis dignissim dui. Cras nec nulla nec nisi tempor aliquet. Donec vitae nibh massa. Suspendisse nulla quam, pretium nec nunc sed, dignissim eleifend lacus. Sed ullamcorper vestibulum justo quis pretium. Quisque tortor est, pretium et vestibulum in, malesuada vitae purus. Phasellus vestibulum congue mattis. Phasellus porta nisl non nunc venenatis mattis. Vestibulum a nulla sed est pharetra pharetra. Proin porta elit eget augue maximus, et cursus enim aliquam. In elementum aliquet elit, tincidunt gravida justo congue in. Phasellus id dignissim libero, quis eleifend eros. Integer cursus sollicitudin sem, sit amet vehicula enim rutrum et. Duis finibus eros neque, non ultricies nibh maximus ut.  Suspendisse nisl urna, cursus ac tempor ut, facilisis vitae massa. Suspendisse eu tempus sem. Nunc posuere vitae nisi ac placerat. Nunc ac nisi tellus. Aenean sagittis enim diam, vel suscipit diam facilisis vel. Etiam metus neque, tempus id tortor vel, placerat commodo urna. Vivamus scelerisque ex nisl, sit amet convallis lectus finibus iaculis. Maecenas pulvinar est lacus, sed ultrices orci vestibulum in. Etiam eleifend porta lectus vel faucibus.  Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus iaculis, metus id mollis volutpat, nulla sem tempor ligula, at ornare sem massa sed tellus. Donec tincidunt tristique laoreet. Proin suscipit gravida erat, eu consequat elit pulvinar in. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Curabitur ultrices ante in libero aliquet sodales. Suspendisse potenti. Maecenas viverra ligula ac pulvinar maximus. Aliquam semper quam dui, sit amet egestas tortor ullamcorper ac.  Donec ullamcorper feugiat sodales. Aenean semper, massa in vehicula lobortis, nisl nunc varius lacus, volutpat vehicula massa purus eget eros. Nullam suscipit lectus augue. Curabitur iaculis, enim ac congue pretium, lacus nibh volutpat felis, rhoncus posuere turpis felis ut libero. Etiam dignissim nibh sem, eget rutrum lorem maximus eu. Pellentesque pellentesque est odio, non tincidunt quam tempor eu. Nam ornare nisi et feugiat aliquam.  Suspendisse interdum lobortis dignissim. Pellentesque id molestie est, sollicitudin pharetra magna. Suspendisse cursus id odio in congue. Donec ut blandit ante. Proin ac sem in orci eleifend auctor. Integer sed sem ut quam aliquet convallis in quis mi. Mauris sagittis odio volutpat turpis vulputate, id scelerisque orci pharetra. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Sed id consectetur dui. Vivamus malesuada mollis tempor.  Aliquam fermentum dictum tortor in accumsan. Integer fringilla lorem aliquet rhoncus gravida. Pellentesque ut magna non urna tempus consequat. Sed tincidunt accumsan cursus. Nullam porttitor, orci at euismod aliquam, eros purus ullamcorper diam, aliquet hendrerit arcu dui sed lacus. Duis hendrerit consectetur mollis. Etiam luctus turpis velit, non sagittis dui accumsan non.  Nunc ac tortor eget neque mollis elementum ut id leo. Nullam tristique porta quam, sed molestie magna tincidunt sit amet. Cras erat mauris, sollicitudin vitae pharetra eu, faucibus et magna. Duis vel felis eu nulla consequat efficitur a id enim. Integer ultricies nulla ac nisl rutrum, eu sollicitudin metus convallis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Praesent purus nisl, aliquam a elit sit amet, fermentum imperdiet ex. Quisque et diam fermentum, volutpat mi id, blandit erat. Sed rhoncus."

  val literals = text.split(" ").zipWithIndex.map{
    case (literal, index) => (index.toString, literal)
  }

  "An Indexer" should "copy documents from one index to another" in {
    import actors._
    import scala.concurrent.ExecutionContext.Implicits.global
    val server1Future = LocalElasticSearchTestServer("test-cluster1", "index1", 10001)
    val server2Future = LocalElasticSearchTestServer("test-cluster2", "index2", 10002)
    val outputStream = new ByteArrayOutputStream()
    val f = server1Future.zip(server2Future).flatMap{
      case (server1, server2) =>
        val g = server1.add(literals).flatMap{
          case _ =>
            Future{
              Indexer.main(Array("-f", "es://localhost:10001/index1/literal", "-t", "es://localhost:10002/index2/literal", "-c","2", "-b", "500"))
            }.flatMap{
              _ =>
                val reIndexedIdsSource = Indexer.getDocumentIds(server2.client, server2.indexName, "literal", () => outputStream)
                val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)
                reIndexedIdsSource
                  .map(t => 1)
                  .toMat(sumSink)(Keep.right).run()
            }
            // local test
            /*Indexer.reindex(server1.client, server1.indexName, "literal", server2.client, server2.indexName, "literal").flatMap{
              _ =>
                val reIndexedIdsSource = Indexer.getDocumentIds(server2.client, server2.indexName, "literal", () => outputStream)
                val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)
                reIndexedIdsSource
                  .map(t => 1)
                  .toMat(sumSink)(Keep.right).run()
            }*/
        }
        g.andThen{
          case _ =>
            server1.close()
            server2.close()
        }
        g
    }
    f.onFailure{
      case t => assert(false)
    }

    val literalCount = Await.result(f, scala.concurrent.duration.Duration.Inf)
    literalCount should be (literals.length)
  }
}
