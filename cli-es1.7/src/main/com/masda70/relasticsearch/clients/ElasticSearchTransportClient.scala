package com.masda70.relasticsearch.clients

import com.masda70.relasticsearch.futures.ListenableActionFutureExtensions._
import com.masda70.relasticsearch.publisher.{SearchHitPublisher, SearchHitPublisherLike}
import com.masda70.relasticsearch.{ElasticSearchAggregateBulkException, ElasticSearchBulkException}
import org.elasticsearch.action.bulk.{BulkRequestBuilder, BulkResponse}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.index.query.QueryBuilders
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.{ExecutionContext, Future}

/**
  * @author David Montoya
  */
class ElasticSearchTransportClient(client: Client)(implicit executionContext: ExecutionContext)
  extends ElasticSearchClient[(String, Either[BytesReference, JObject]), SearchRequestBuilder, (String, String, BulkRequestBuilder), BulkResponse] {

  def close() = client.close()

  override def searchHitPublisher(scrollTime: Long, scrollSize: Int, query: SearchRequestBuilder): SearchHitPublisherLike[(String, Either[BytesReference, JObject])] = {
    new SearchHitPublisher(client, scrollTime, scrollSize, query)
  }

  override def searchHitId(searchHit: (String, Either[BytesReference, JObject])): String = searchHit._1

  override def responseSink(response: BulkResponse): Unit = {
    val exceptions = response.getItems.filter(_.isFailed).map {
      item => new ElasticSearchBulkException(item.getIndex, item.getType, item.getId, item.getFailureMessage)
    }
    if (exceptions.nonEmpty) {
      val exception = new ElasticSearchAggregateBulkException("Bulk exceptions:", exceptions)
      logger.error(exception.getMessage)
      throw exception
    }
  }

  override def prepareBulk(index: String, `type`: String): (String, String, BulkRequestBuilder) = {
    (index, `type`, client.prepareBulk())
  }

  override def addToBulk(bulk: (String, String, BulkRequestBuilder), searchHit: (String, Either[BytesReference, JObject])): Unit = {
    val query = client.prepareIndex(bulk._1, bulk._2).setId(searchHit._1)
    searchHit._2 match {
      case Left(bytesReference) => query.setSource(bytesReference)
      case Right(json) => query.setSource(compact(json))
    }
    bulk._3.add(query)
  }

  override def executeBulk(bulk: (String, String, BulkRequestBuilder)): Future[BulkResponse] = {
    bulk._3.execute().future
  }

  override def refreshIndex(index: String): Future[Unit] = {
    client.admin().indices().prepareRefresh(index).execute().future.map(_ => ())
  }

  override def countDocuments(index: String, `type`: String): Future[Long] = {
    matchAllQuery(index, `type`, fetchSource = false)
      .setSize(0)
      .execute()
      .future.map {
      response => response.getHits.getTotalHits
    }
  }

  override def matchAllQuery(index: String, `type`: String, fetchSource: Boolean): SearchRequestBuilder = {
    client.prepareSearch(index)
      .setSearchType(SearchType.SCAN)
      .setTypes(`type`)
      .setQuery(QueryBuilders.matchAllQuery())
      .setFetchSource(fetchSource)
  }
}
