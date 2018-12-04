package edu.gatech.cse6250.utils

import org.joda.time.DateTime

object DateUtils {
  val HOURMILLIS = 1000 * 60 * 60
  val DAYMILLIS = HOURMILLIS * 24
  val YEARMILLIS = DAYMILLIS * 365

  def quantizeToHour(time: Long): Long = {
    new DateTime(time).
      withMinuteOfHour(0).
      withSecondOfMinute(0).
      withMillisOfSecond(0).
      getMillis
  }

  def quantizeToDay(time: Long): Long = {
    new DateTime(time).
      withTimeAtStartOfDay.
      getMillis
  }
}
