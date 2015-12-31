# relasticsearch

Reindexing Tool for [ElasticSearch](https://www.elastic.co) written in [Scala](http://www.scala-lang.org/) using [Akka Streams](http://doc.akka.io/docs/akka-stream-and-http-experimental/2.0.1/scala.html).

Uses ElasticSearch's Java Transport Client v1.7.4.

Built using [SBT](http://www.scala-sbt.org/).

## Usage
```Usage: relasticsearch [options]
 -f <uri> | --from <uri>
        source transport address/index/type, e.g. es://192.168.1.123:9300/old_index/old_type
 -t <uri> | --to <uri>
        destination transport address/index/type, e.g. es://192.168.1.123:9301/new_index/new_type
 -c <value> | --concurrency <value>
        indexing concurrency
 -b <value> | --bulkSize <value>
        query and index bulk size
```