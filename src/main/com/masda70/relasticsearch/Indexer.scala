package com.masda70.relasticsearch

import java.io.OutputStream

import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Flow, Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHits
import futures.ListenableActionFutureExtensions._
import java.net.URI
import java.net.InetSocketAddress;
import scala.concurrent.{Future, Await, ExecutionContext}

/**
  * @author David Montoya
  */
object Indexer extends StrictLogging{

  case class Config(from: URI = null, to: URI = null, bulkSize: Int = 500, concurrency: Int = 2)

  def validateAddress(optionParser: scopt.OptionParser[Config], optionName: String, source: URI) = {
    Option(source.getScheme).map(_.toLowerCase) match{
      case Some("es") =>
        if(source.getPort <= 0){
          optionParser.failure(s"""Option --$optionName's URI port is invalid""")
        } else{
          Option(source.getHost) match{
            case Some(host) =>
              Option(source.getPath) match{
                case Some(path) if path.split("/").length == 3 => optionParser.success
                case path => optionParser.failure(s"""Option --$optionName's URI path must be of the form /<index>/<type>, received "${path.getOrElse("")}"""")
              }
            case host => optionParser.failure(s"""Option --$optionName's URI host is invalid, received "${host.getOrElse("")}"""")
          }
        }
      case _ => optionParser.failure(s"Option --$optionName's URI scheme must be `es`")
    }
  }

  private val parser = new scopt.OptionParser[Config]("relasticsearch") {
    head("relasticsearch")
    opt[URI]('f', "from") required() valueName "<uri>" action { (x, c) =>
      c.copy(from = x) } text "source transport address/index/type, e.g. es://192.168.1.123:9300/old_index/old_type" validate {
        x => validateAddress(this, "from", x)
      }
    opt[URI]('t', "to") required() valueName "<uri>" action { (x, c) =>
      c.copy(to = x) } text "destination transport address/index/type, e.g. es://192.168.1.123:9301/new_index/new_type" validate {
        x => validateAddress(this, "to", x)
      }
    opt[Int]('c', "concurrency") valueName "<value>" action { (x, c) =>
      c.copy(concurrency = x) } text "indexing concurrency" validate { x =>
      if (x > 0) success else failure("Option --concurrency must be > 0") }
    opt[Int]('b', "bulkSize") valueName "<value>" action { (x, c) =>
      c.copy(bulkSize = x) } text "query and index bulk size" validate { x =>
      if (x > 0) success else failure("Option --bulkSize must be > 0") }
  }

  def client(host: String, port: Int) = {
    val sBuilder = Settings.builder()
    sBuilder.put("client.transport.ignore_cluster_name", true)
    val settings: Settings = sBuilder.build
    val client = TransportClient.builder().settings(settings).build()
    try {
      client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, port)))
    }catch{
      case e:Exception =>
        client.close()
        throw e
    }
    client
  }

  def getDocumentIds(client: Client, index: String,
                     docType: String, createOutputStream: () => OutputStream,
                     concurrency: Int = 2,
                     scrollSize: Int = 500)(implicit system: ActorSystem, executionContext: ExecutionContext) = {
    // Duration the scroll is kept by ElasticSearch in the absence of activity
    val scrollTime = new TimeValue(600000)

    // Setup the query
    val query = client.prepareSearch(index)
      .setTypes(docType)
      .setQuery(QueryBuilders.matchAllQuery())

    // SearchHits Source
    val documentSource = Source.actorPublisher[SearchHits](Props(new SearchHitPublisher(client, query, scrollTime, scrollSize))).map {
      case searchHits => searchHits.getHits
    }

    documentSource.map(_.map(_.id)(scala.collection.breakOut): scala.collection.immutable.Seq[String]).mapConcat(identity)

  }

  def reindex(fromClient: Client, fromIndex: String, fromType: String,
              toClient: Client, toIndex: String, toType: String,
              concurrency: Int = 2,
              scrollSize: Int = 500,
              idFilter: String => Boolean = _ => true)(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) = {
    // Duration the scroll is kept by ElasticSearch in the absence of activity
    val scrollTime = new TimeValue(600000)

    // Setup the query
    val query = fromClient.prepareSearch(fromIndex)
      .setTypes(fromType)
      .setQuery(QueryBuilders.matchAllQuery())

    // SearchHits Source
    val documentSource = Source.actorPublisher[SearchHits](Props(new SearchHitPublisher(fromClient, query, scrollTime, scrollSize))).map{
      case searchHits => searchHits.getHits
    }

    // Filter SearchHits by id
    val filteredDocumentSource = documentSource.map{
      _.filter(x => idFilter(x.id()))
    }.filter(_.nonEmpty)

    val bulkResponses = filteredDocumentSource.mapAsyncUnordered(concurrency){
      case searchHits =>
        val bulkReq = toClient.prepareBulk()
        searchHits.foreach {
          case hit =>
            bulkReq.add(toClient.prepareIndex(toIndex, toType).setId(hit.id()).setSource(hit.getSourceRef))
        }
        val future = bulkReq.execute().future
        future
    }

    val future = bulkResponses.runWith(Sink.foreach{
      response => ()
        if(response.hasFailures){
          logger.error(s"Failures during Bulk Indexing: ${response.buildFailureMessage()}")
        }
    }).flatMap{
      _ => toClient.admin().indices().prepareRefresh(toIndex).execute().future.map(_ => ())
    }
    future.onFailure{
      case t => logger.error(s"Error during reindexing: ${helpers.ExceptionUtils.getStackTrace(t)}")
    }
    future
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match{
      case Some(config) =>
        (config.from.getPath.split("/"), config.to.getPath.split("/")) match{
          case (Array(_, fromIndex, fromType), Array(_, toIndex, toType)) =>
            try {
              val fromClient = client(config.from.getHost, config.from.getPort)
              val toClient = client(config.to.getHost, config.to.getPort)
              import actors._
              try {
                logger.info(s"Reindexing from ${config.from.getHost}:${config.from.getPort}/$fromIndex/$fromType to ${config.to.getHost}:${config.to.getPort}/$toIndex/$toType...")
                import scala.concurrent.ExecutionContext.Implicits.global
                val future = reindex(fromClient, fromIndex, fromType, toClient, toIndex, toType)
                Await.ready(future.map {
                  case result =>
                    logger.info(s"Done reindexing.")
                }, scala.concurrent.duration.Duration.Inf)
                ()
              } finally {
                fromClient.close()
                toClient.close()
                materializer.shutdown()
                system.shutdown()
              }
            } catch {
              case e:Exception =>
                logger.error(s"Error: ${e.getMessage}")
            }
          case _ => assert(assertion = false, "expected URIs with path of type /<index>/<type>. This should have failed earlier.")
        }
      case None =>
    }
  }
}
