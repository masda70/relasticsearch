package com.masda70.relasticsearch.clients

import akka.actor.ActorSystem
import com.masda70.relasticsearch.publisher.SearchHitPublisherLike
import com.masda70.relasticsearch.ws
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import org.json4s.JsonDSL._
import org.json4s.{Formats, JObject, _}
import spray.client.pipelining._
import spray.http.Uri
import spray.httpx.unmarshalling._

import scala.concurrent.{ExecutionContext, Future}

/**
  * @author David Montoya
  */
class ElasticSearchHttpClient(host: String, port: Int)
                             (implicit executionContext: ExecutionContext, actorSystem: ActorSystem)
  extends ElasticSearchClient[(String, JObject), (Long, Int) => Future[(String, Long, Vector[(String, JObject)])], (String, String, ByteOutputStream), Unit] {
  val simpleFormats = org.json4s.DefaultFormats
  private val client = ws.client
  private val esUri = Uri.from(scheme = "http", host = host, port = port)

  def close(): Unit = {

  }

  override def searchHitId(searchHit: (String, JObject)): String = searchHit._1

  override def searchHitPublisher(scrollTime: Long, scrollSize: Int, query: (Long, Int) => Future[(String, Long, Vector[(String, JObject)])]): SearchHitPublisherLike[(String, JObject)] = {
    def scanScroll(scrollId: String) = {
      val url = esUri.withPath(Uri.Path./("_search") / "scroll").withQuery(s"scroll=${scrollTime}ms")
      val request = Get(uri = url, scrollId)
      import SimpleApiMarshallers._
      val pipeline = client ~> unmarshal[JObject]
      pipeline {
        request
      }.map(parseResponse)
    }

    new SearchHitPublisherLike[(String, JObject)] {
      override def queryBuilder = {
        case Some(scrollId) => () => scanScroll(scrollId)
        case None => () => query(scrollTime, scrollSize)
      }

      override implicit val executionContext: ExecutionContext = ElasticSearchHttpClient.this.executionContext
    }
  }

  override def refreshIndex(index: String): Future[Unit] = Future()

  override def matchAllQuery(index: String, `type`: String, fetchSource: Boolean) = {
    case (scrollTime: Long, scrollSize: Int) =>
      import SimpleApiMarshallers._
      val scanSearch = true
      val baseBody = if (!fetchSource) {
        ("_source" -> false) ~ Nil
      } else {
        Nil
      }: JObject
      (if (scanSearch) {
        val url = esUri.withPath(Uri.Path./(index) / `type` / "_search").withQuery(s"scroll=${scrollTime}ms&search_type=scan")
        getNumberOfShards(index, `type`).map {
          shardCount =>
            val body = ("size" -> scrollSize / shardCount) ~ baseBody
            (url, body)
        }
      } else {
        val url = esUri.withPath(Uri.Path./(index) / `type` / "_search").withQuery(s"scroll=${scrollTime}ms")
        val body = ("sort" -> List("_doc")) ~ ("size" -> scrollSize) ~ baseBody
        Future.successful((url, body))
      }).flatMap {
        case (url, body) =>
          val request = Get(uri = url, body)
          val pipeline = client ~> unmarshal[JObject]
          pipeline {
            request
          }.map(parseResponse)
      }
  }

  def parseResponse(response: JObject) = {
    checkError(response)
    val scrollId = (response \ "_scroll_id").asInstanceOf[JString].s
    val hits = response \ "hits"
    val totalHits = (hits \ "total").asInstanceOf[JInt].num.toLong
    val hitsHits = (hits \ "hits").asInstanceOf[JArray]
    (scrollId, totalHits, hitsHits.arr.map {
      case hit =>
        val source = hit \ "_source" match {
          case o: JObject => o
          case JNothing => JObject()
        }
        ((hit \ "_id").asInstanceOf[JString].s, source)
    }(scala.collection.breakOut): Vector[(String, JObject)])
  }

  def checkError(response: JObject) = {
    response \ "timed_out" match {
      case JBool(true) =>
        logger.error(s"Timed out response: $response")
      case _ =>
    }
    response \ "_shards" \ "failed" match {
      case JInt(v) if v > 0 =>
        logger.error(s"Failed shards: $response")
      case _ =>
    }
  }

  private def getNumberOfShards(index: String, `type`: String): Future[Long] = {
    import SimpleApiMarshallers._
    val url = esUri.withPath(Uri.Path./(index) / `type` / "_count")
    val request = Get(uri = url)
    val pipeline = client ~> unmarshal[JObject]
    pipeline {
      request
    }.map {
      response =>
        (response \ "_shards" \ "total").asInstanceOf[JInt].num.toInt
    }
  }

  override def responseSink(response: Unit): Unit = {}

  override def executeBulk(bulk: (String, String, ByteOutputStream)): Future[Unit] = {
    bulk match {
      case (index, _type, stream) =>
        val url = esUri.withPath(Uri.Path./(index) / _type / "_bulk")
        val request = Post(uri = url, stream.getBytes)
        val pipeline = client
        pipeline {
          request
        }.map {
          _ => ()
        }
    }
  }

  override def addToBulk(bulk: (String, String, ByteOutputStream), searchHit: (String, JObject)): Unit = {
    val stream = bulk._3
    implicit val formats = simpleFormats
    org.json4s.jackson.Serialization.write(("index" -> ("_id" -> searchHit._1)) ~ Nil, stream)
    stream.write("\n".getBytes("UTF-8"))
    org.json4s.jackson.Serialization.write(searchHit._2, stream)
    stream.write("\n".getBytes("UTF-8"))
  }

  override def prepareBulk(index: String, `type`: String): (String, String, ByteOutputStream) = {
    val stream = new ByteOutputStream()
    (index, `type`, stream)
  }

  override def countDocuments(index: String, `type`: String): Future[Long] = {
    import SimpleApiMarshallers._
    val url = esUri.withPath(Uri.Path./(index) / `type` / "_count")
    val request = Get(uri = url)
    val pipeline = client ~> unmarshal[JObject]
    pipeline {
      request
    }.map {
      response =>
        (response \ "count").asInstanceOf[JInt].num.toLong
    }
  }

  object SimpleApiMarshallers extends spray.httpx.Json4sJacksonSupport {
    override implicit def json4sJacksonFormats: Formats = simpleFormats
  }

}
