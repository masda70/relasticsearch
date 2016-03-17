package com.masda70.relasticsearch

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.masda70.relasticsearch.clients.{ElasticSearchHttpClient, ElasticSearchTransportClient}
import com.masda70.relasticsearch.utilities.TimeExecution
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.mapdb.DB

import scala.concurrent.{ExecutionContext, Future}

/**
  * @author David Montoya
  */
object CommandLine extends CommandLineTrait[Either[ElasticSearchHttpClient, ElasticSearchTransportClient]] {


  override def client(scheme: String, host: String, port: Int)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem): Either[ElasticSearchHttpClient, ElasticSearchTransportClient] = {
    scheme match {
      case "http" => Left(new ElasticSearchHttpClient(host, port))
      case "es" =>
        val sBuilder = Settings.builder()
        sBuilder.put("client.transport.ignore_cluster_name", true)
        val settings: Settings = sBuilder.build
        val client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, port)))
        Right(new ElasticSearchTransportClient(client))
    }
  }

  override def reindex(db: DB,
                       fromClient: Either[ElasticSearchHttpClient, ElasticSearchTransportClient],
                       fromIndex: String,
                       fromType: String,
                       toClient: Either[ElasticSearchHttpClient, ElasticSearchTransportClient],
                       toIndex: String,
                       toType: String,
                       scrollSize: Int,
                       concurrency: Int)(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Future[Unit] = {
    val indexedIds = indexedIdsSet(db)
    (toClient match {
      case Left(toHttpClient) =>
        toHttpClient.countDocuments(toIndex, toType)
      case Right(toTransportClient) =>
        toTransportClient.countDocuments(toIndex, toType)
    }).flatMap {
      count =>
        logger.info(s"Scanning $count documents in destination node.")
        TimeExecution.timeProgress("building-filter", count, logger, {
          progress =>
            (toClient match {
              case Left(toHttpClient) =>
                toHttpClient.documentIds(toIndex, toType, scrollSize)
              case Right(toTransportClient) =>
                toTransportClient.documentIds(toIndex, toType, scrollSize)
            }).grouped(scrollSize).mapAsyncUnordered(parallelism = 4) {
              case (ids) =>
                Future {
                  progress(ids.size)
                  ids.foreach {
                    id => indexedIds.put(id, true)
                  }
                  true
                }
            }.runForeach(_ => ())
        })
    }.flatMap {
      dbMaker =>
        val idFilter = (id: String) => !indexedIds.containsKey(id)
        fromClient match {
          case Left(fromHttpClient) =>
            fromHttpClient.countDocuments(fromIndex, fromType).flatMap {
              count =>
                logger.info(s"Indexing $count documents from source to destination node [dbSize=${indexedIds.sizeLong()}.")
                TimeExecution.timeProgress("indexing", count, logger, {
                  case progress =>
                    val source = fromHttpClient.queryDocuments(fromIndex, fromType, scrollSize, idFilter, progress = progress)
                    toClient match {
                      case Left(toHttpClient) =>
                        toHttpClient.indexDocuments(source, toIndex, toType, scrollSize, concurrency)
                      case Right(toTransportClient) =>
                        val documents = source.map {
                          case (id, sourceJson) =>
                            (id, Right(sourceJson))
                        }
                        toTransportClient.indexDocuments(documents, toIndex, toType, scrollSize, concurrency)
                    }
                })
            }
          case Right(fromTransportClient) =>
            fromTransportClient.countDocuments(fromIndex, fromType).flatMap {
              count =>
                logger.info(s"Indexing $count documents from source to destination node.")
                TimeExecution.timeProgress("indexing", count, logger, {
                  case progress =>
                    val source = fromTransportClient.queryDocuments(fromIndex, fromType, scrollSize, idFilter, progress = progress)
                    toClient match {
                      case Left(toHttpClient) =>
                        val documents = source.map {
                          case (id, Left(bytesReference)) =>
                            (id, parse(bytesReference.streamInput).asInstanceOf[JObject])
                          case (id, Right(json)) =>
                            (id, json)
                        }
                        toHttpClient.indexDocuments(documents, toIndex, toType, scrollSize, concurrency)
                      case Right(toTransportClient) =>
                        toTransportClient.indexDocuments(source, toIndex, toType, scrollSize, concurrency)
                    }
                })
            }
        }
    }.andThen {
      case _ => indexedIds.close()
    }
  }

  override def closeClient(client: Either[ElasticSearchHttpClient, ElasticSearchTransportClient]): Unit = {
    client match {
      case Left(c) => c.close()
      case Right(c) => c.close()
    }
  }
}
