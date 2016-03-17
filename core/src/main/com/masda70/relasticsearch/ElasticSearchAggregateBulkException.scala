package com.masda70.relasticsearch

/**
  * @author David Montoya
  */
class ElasticSearchAggregateBulkException(bulkErrorMessage: String, exceptions: Seq[ElasticSearchBulkException])
  extends Exception((bulkErrorMessage +: exceptions.map(_.getMessage)).mkString("\n")) {

}
