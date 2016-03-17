package com.masda70.relasticsearch

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import org.mapdb.DB

import scala.concurrent.{Await, ExecutionContext, Future}

case class CommandLineConfig(from: URI = null,
                             to: URI = null,
                             bulkSize: Int = 500,
                             concurrency: Int = 2)

/**
  * @author David Montoya
  */
trait CommandLineTrait[CLIENT] extends StrictLogging {

  private val parser = new scopt.OptionParser[CommandLineConfig]("relasticsearch") {
    head("relasticsearch")
    opt[URI]('f', "from") required() valueName "<uri>" action { (x, c) =>
      c.copy(from = x)
    } text "source transport address/index/type, e.g. es://192.168.1.123:9300/old_index/old_type" validate {
      x => validateAddress(this, "from", x)
    }
    opt[URI]('t', "to") required() valueName "<uri>" action { (x, c) =>
      c.copy(to = x)
    } text "destination transport address/index/type, e.g. es://192.168.1.123:9301/new_index/new_type" validate {
      x => validateAddress(this, "to", x)
    }
    opt[Int]('c', "concurrency") valueName "<value>" action { (x, c) =>
      c.copy(concurrency = x)
    } text "indexing concurrency" validate { x =>
      if (x > 0) success else failure("Option --concurrency must be > 0")
    }
    opt[Int]('b', "bulkSize") valueName "<value>" action { (x, c) =>
      c.copy(bulkSize = x)
    } text "query and index bulk size" validate { x =>
      if (x > 0) success else failure("Option --bulkSize must be > 0")
    }
  }

  def client(scheme: String, host: String, port: Int)(implicit executionContext: ExecutionContext, actorSystem: ActorSystem): CLIENT

  def closeClient(client: CLIENT): Unit

  def indexedIdsSet(db: DB) = {
    db.treeMap(UUID.randomUUID().toString, org.mapdb.Serializer.STRING, org.mapdb.Serializer.BOOLEAN).create()
  }

  def validateAddress(optionParser: scopt.OptionParser[CommandLineConfig], optionName: String, source: URI) = {
    Option(source.getScheme).map(_.toLowerCase) match {
      case Some("es") | Some("http") =>
        if (source.getPort <= 0) {
          optionParser.failure(s"""Option --$optionName's URI port is invalid""")
        } else {
          Option(source.getHost) match {
            case Some(host) =>
              Option(source.getPath) match {
                case Some(path) if path.split("/").length == 3 => optionParser.success
                case path => optionParser.failure(s"""Option --$optionName's URI path must be of the form /<index>/<type>, received "${path.getOrElse("")}"""")
              }
            case host => optionParser.failure(s"""Option --$optionName's URI host is invalid, received "${host.getOrElse("")}"""")
          }
        }
      case _ => optionParser.failure(s"Option --$optionName's URI scheme must be `es` or `http`")
    }
  }

  def reindex(db: DB,
              fromClient: CLIENT, fromIndex: String, fromType: String,
              toClient: CLIENT, toIndex: String, toType: String,
              scrollSize: Int = 500,
              concurrency: Int = 2)(implicit system: ActorSystem, materializer: Materializer, executionContext: ExecutionContext): Future[Unit]

  def main(args: Array[String]): Unit = {
    parser.parse(args, CommandLineConfig()) match {
      case Some(config) =>
        (config.from.getPath.split("/"), config.to.getPath.split("/")) match {
          case (Array(_, fromIndex, fromType), Array(_, toIndex, toType)) =>
            try {
              import actors._

              import scala.concurrent.ExecutionContext.Implicits.global
              val fromClient = client(config.from.getScheme, config.from.getHost, config.from.getPort)
              val toClient = client(config.to.getScheme, config.to.getHost, config.to.getPort)
              val (path, db) = makeDB()
              try {
                logger.info(s"Reindexing from ${config.from.getHost}:${config.from.getPort}/$fromIndex/$fromType to ${config.to.getHost}:${config.to.getPort}/$toIndex/$toType...")
                val future = reindex(db, fromClient, fromIndex, fromType, toClient, toIndex, toType, config.bulkSize, config.concurrency)
                future.onFailure {
                  case t =>
                    logger.error(s"Error while reindexing documents: ${helpers.ExceptionUtils.getStackTrace(t)}")
                }
                Await.ready(future.map {
                  case result =>
                    logger.info(s"Done reindexing.")
                }, scala.concurrent.duration.Duration.Inf)
                ()
              } finally {
                closeClient(fromClient)
                closeClient(toClient)
                materializer.shutdown()
                system.shutdown()
                deleteDB(path, db)
              }
            } catch {
              case e: Exception =>
                logger.error(s"Error: ${helpers.ExceptionUtils.getStackTrace(e)}")
            }
          case _ => assert(assertion = false, "expected URIs with path of type /<index>/<type>. This should have failed earlier.")
        }
      case None =>
    }
  }

  def makeDB() = {
    val path = new File("tmp", UUID.randomUUID().toString + ".db").toPath
    Files.createDirectories(path.getParent)
    Files.delete(path)
    (path, org.mapdb.DBMaker.fileDB(path.toString).make())
  }

  def deleteDB(path: Path, db: DB) = {
    db.close()
    Files.delete(path)
  }
}
