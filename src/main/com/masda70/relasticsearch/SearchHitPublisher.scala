package com.masda70.relasticsearch

import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.StrictLogging
import org.elasticsearch.action.search.{SearchResponse, SearchRequestBuilder}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.SearchHits
import futures.ListenableActionFutureExtensions._

import scala.concurrent.{Future, ExecutionContext}

/**
  * @author David Montoya
  */
class SearchHitPublisher(client: Client,
                         query: SearchRequestBuilder,
                         scrollTime: TimeValue,
                         scrollSize: Int)(implicit executionContext: ExecutionContext) extends ActorPublisher[SearchHits] with StrictLogging {
  val handlingRequest = new AtomicBoolean(false)
  @volatile var executeQuery = () => query.setScroll(scrollTime).setSize(scrollSize).execute.future

  def nextHitsCheck() = {
    if(handlingRequest.compareAndSet(false, true)) {
      nextHits()
    }
  }

  def nextHits(): Unit = {
    val future = executeQuery()
    future.foreach{
      response =>
        if(response.getHits.hits.nonEmpty){
          if(isActive && totalDemand > 0){
            executeQuery = () => client.prepareSearchScroll(response.getScrollId).setScroll(scrollTime).execute().future
            nextHits()
            onNext(response.getHits)
          }else{
            handlingRequest.set(false)
          }
        }else{
          onComplete()
          handlingRequest.set(false)
        }

    }
    future.onFailure{
      case t =>
        handlingRequest.set(false)
        throw t
    }
  }

  def receive = {
    case Request(cnt) =>                                                             // 2
      nextHitsCheck()
    case Cancel =>                                                                   // 3
      context.stop(self)
    case _ =>
  }

}
