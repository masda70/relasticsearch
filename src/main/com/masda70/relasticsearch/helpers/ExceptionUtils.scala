package com.masda70.relasticsearch.helpers

import java.io.{PrintWriter, StringWriter}

/**
  * @author David Montoya
  */
object ExceptionUtils {
  def getStackTrace(throwable: Throwable): String = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw, true)
    throwable.printStackTrace(pw)
    sw.getBuffer.toString
  }
}
