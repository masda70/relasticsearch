package com.masda70.relasticsearch.clients

import akka.actor.{ActorSystem, Props}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.masda70.relasticsearch.publisher.SearchHitPublisherLike
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}

/**
  * @author David Montoya
  */
trait ElasticSearchClient[SEARCH_HIT, SEARCH_REQUEST_BUILDER, BULK_REQUEST_BUILDER, BULK_RESPONSE] extends StrictLogging {

  def close(): Unit

  def searchHitId(searchHit: SEARCH_HIT): String

  def searchHitPublisher(scrollTime: Long, scrollSize: Int, query: SEARCH_REQUEST_BUILDER): SearchHitPublisherLike[SEARCH_HIT]

  def matchAllQuery(index: String, `type`: String, fetchSource: Boolean): SEARCH_REQUEST_BUILDER

  def responseSink(response: BULK_RESPONSE): Unit

  def refreshIndex(index: String): Future[Unit]

  def prepareBulk(index: String, `type`: String): BULK_REQUEST_BUILDER

  def addToBulk(bulk: BULK_REQUEST_BUILDER, searchHit: SEARCH_HIT): Unit

  def executeBulk(bulk: BULK_REQUEST_BUILDER): Future[BULK_RESPONSE]

  def countDocuments(index: String, `type`: String): Future[Long]

  def documentIds(index: String,
                  `type`: String,
                  scrollSize: Int = 500)(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) = {
    queryDocuments(index, `type`, scrollSize, fetchSource = false).map {
      document => searchHitId(document)
    }
  }

  def queryDocuments(fromIndex: String,
                     fromType: String,
                     scrollSize: Int = 500,
                     idFilter: String => Boolean = _ => true,
                     fetchSource: Boolean = true,
                     progress: Long => Unit = _ => ())(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) = {

    // Setup the query
    val query = matchAllQuery(fromIndex, fromType, fetchSource = fetchSource)

    // SearchHits Source
    val documentSource = Source.actorPublisher[Vector[SEARCH_HIT]](Props(searchHitPublisher(scrollTime, scrollSize, query))).mapConcat {
      hits =>
        progress(hits.length)
        hits.filter {
          case x => idFilter(searchHitId(x))
        }
    }

    documentSource
  }

  def scrollTime: Long = 3000000

  def indexDocuments[T](source: Source[SEARCH_HIT, T],
                        toIndex: String,
                        toType: String,
                        bulkSize: Int = 500,
                        concurrency: Int = 2,
                        progress: Long => Unit = _ => ())(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) = {
    val bulkResponses = source.grouped(bulkSize).mapAsyncUnordered(concurrency) {
      case searchHits =>
        val bulkReq = prepareBulk(toIndex, toType)
        searchHits.foreach {
          case searchHit =>
            addToBulk(bulkReq, searchHit)
        }
        val future = executeBulk(bulkReq)
        progress(searchHits.length)
        future
    }

    val future = bulkResponses.runWith(Sink.foreach {
      response =>
        responseSink(response)
    }).flatMap {
      _ => refreshIndex(toIndex)
    }

    future
  }


}
