package com.masda70.relasticsearch.publisher

import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/**
  * @author David Montoya
  */
trait SearchHitPublisherLike[SEARCH_HIT]
  extends ActorPublisher[Vector[SEARCH_HIT]] with StrictLogging {
  var executeQuery = queryBuilder(None)
  var hitCount = 0
  var noMoreHits = false
  var processing = false
  var buf = Vector.empty[Vector[SEARCH_HIT]]

  def queryBuilder: (Option[String]) => (() => Future[(String, Long, Vector[SEARCH_HIT])])

  implicit def executionContext: ExecutionContext

  def nextHits(): Unit = {
    try {
      if (!processing) {
        processing = true
        val future = executeQuery()
        future.foreach {
          case (scrollId, totalHits, searchHits) =>
            this.self ! Result(scrollId, totalHits, searchHits)
        }
        future.onFailure {
          case t => this.self ! Failure(t)
        }
      }
    } catch {
      case t: Exception => this.self ! Failure(t)
    }
  }

  def receive = {
    case Result(scrollId, totalHits, hits) =>
      processing = false
      executeQuery = queryBuilder(Some(scrollId))
      hitCount += hits.length
      if (hitCount >= totalHits) {
        noMoreHits = true
      }
      if (buf.isEmpty && isActive && totalDemand > 0)
        onNext(hits)
      else {
        buf :+= hits
        deliverBuf()
      }
      if (buf.isEmpty && noMoreHits) {
        onComplete()
      } else {
        if (isActive && totalDemand > 0) {
          nextHits()
        }
      }
    case Failure(failure) =>
      onError(failure)
      processing = false
    case Request(cnt) =>
      deliverBuf()
      if (isActive) {
        if (noMoreHits) {
          onComplete()
        } else if (totalDemand > 0) {
          nextHits()
        }
      }
    case Cancel =>
      context.stop(self)
    case _ =>
  }

  @tailrec final def deliverBuf(): Unit =
    if (isActive && totalDemand > 0) {
      /*
       * totalDemand is a Long and could be larger than
       * what buf.splitAt can accept
       */
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use foreach onNext
        deliverBuf()
      }
    }

  case class Result(scrollId: String, totalHits: Long, hits: Vector[SEARCH_HIT])

  case class Failure(throwable: Throwable)

}
