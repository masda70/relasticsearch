package com.masda70.relasticsearch.utilities

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.Logger

/**
  * @author David Montoya
  */
object TimeExecution {

  implicit def durationWithToSeconds(d: Duration): DurationWithToSeconds = new DurationWithToSeconds(d)

  implicit def doubleWithToDurationAsSeconds(d: Double): DurationAsSeconds = new DurationAsSeconds(d)

  def timeProgress[R](processName: String, target: Long, logger: Logger, block: (Long => Unit) => R): R = {
    var lastProgress = (0L, System.nanoTime())
    var done = false
    val beginTime = System.nanoTime()
    val period = Duration.ofSeconds(5)
    val current = new AtomicLong(0)
    val progress = (delta: Long) => {
      val p = current.addAndGet(delta)
      val percent = (p.toDouble / target.toDouble) * 100.0
      val currentTime = System.nanoTime()
      val b = if (!done && p == target) {
        done = true
        true
      } else {
        false
      }
      if (b || Duration.ofNanos(currentTime - lastProgress._2).compareTo(period) >= 0) {
        val processDuration = Duration.ofNanos(currentTime - beginTime)
        val averageSpeed = 1d / processDuration.dividedBy(p).toSecondsDouble
        if (b) {
          logger.info(f"[$processName] - $percent%.2f%% (average_speed=$averageSpeed%.2f ops/s, time=$processDuration}).")
        } else {
          val currentSpeed = 1d / Duration.ofNanos(currentTime - lastProgress._2).dividedBy(p - lastProgress._1).toSecondsDouble
          val speed = 0.5 * averageSpeed + 0.5 * currentSpeed
          val eta = ((target - p).toDouble / speed).ceil.asSecondsDuration
          logger.info(f"[$processName] - $percent%.2f%% ($speed%.2f ops/s, eta=$eta}.")
        }
        lastProgress = (p, currentTime)
      }
    }
    block(progress)
  }

  class DurationWithToSeconds(val d: Duration) {
    def toSecondsDouble: Double = {
      d.getSeconds.toDouble + d.getNano.toDouble / 1000000000.0d
    }
  }

  class DurationAsSeconds(val d: Double) {
    def asSecondsDuration: Duration = {
      val t = d.toLong
      val remainder = ((d - t) * 1000000000.0d).toLong
      Duration.ofSeconds(t, remainder)
    }
  }

}
