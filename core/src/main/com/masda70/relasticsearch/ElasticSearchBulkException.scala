package com.masda70.relasticsearch

/**
  * @author David Montoya
  */
class ElasticSearchBulkException(index: String, t: String, id: String, bulkErrorMessage: String)
  extends Exception(s"index [$index], type [$t], id [$id], bulkErrorMessage [$bulkErrorMessage]") {
}
