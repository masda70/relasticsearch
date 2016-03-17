package com.masda70.relasticsearch.publisher

import com.masda70.relasticsearch.futures.ListenableActionFutureExtensions._
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.unit.TimeValue
import org.json4s.JsonAST.JObject

import scala.concurrent.ExecutionContext

/**
  * @author David Montoya
  */
class SearchHitPublisher(client: Client,
                         scrollTime: Long,
                         scrollSize: Int,
                         query: SearchRequestBuilder)(implicit val executionContext: ExecutionContext)
  extends SearchHitPublisherLike[(String, Either[BytesReference, JObject])] {

  val scrollTimeValue = new TimeValue(scrollTime)

  def queryBuilder = {
    case Some(scrollId) =>
      () => client.prepareSearchScroll(scrollId).setScroll(scrollTimeValue).execute().future.map(searchResponse)
    case None =>
      () => query.setScroll(scrollTimeValue).setSize(scrollSize).execute.future.map(searchResponse)
  }

  def searchResponse(response: SearchResponse) = {
    (response.getScrollId, response.getHits.getTotalHits, response.getHits.getHits.toVector.map {
      hit =>
        if (hit.isSourceEmpty) {
          (hit.id(), Right(JObject()))
        } else {
          (hit.id(), Left(hit.getSourceRef))
        }

    })
  }

}
