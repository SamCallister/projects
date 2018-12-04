package edu.gatech.cse6250.utils

object Collectors {

  object MinMaxCollector {
    def empty(lastChartTime: Long): MinMaxCollector = {
      new MinMaxCollector(None, None, lastChartTime)
    }
    def withValue(value: Double, chartTime: Long): MinMaxCollector = {
      new MinMaxCollector(value, chartTime: Long)
    }
  }

  class MinMaxCollector(val minVal: Option[Double], val maxVal: Option[Double], val lastChartTime: Long) {
    def this(initial: Option[Double], chartTime: Long) {
      this(initial, initial, chartTime)
    }

    def this(initial: Double, chartTime: Long) {
      this(Some(initial), Some(initial), chartTime)
    }

    def combine(that: MinMaxCollector): MinMaxCollector = {
      val newMin = this.minVal match {
        case Some(v1) =>
          that.minVal match {
            case Some(v2) => if (v2 < v1) that.minVal else this.minVal
            case _ => this.minVal
          }
        case _ => that.minVal
      }
      val newMax = this.maxVal match {
        case Some(v1) => {
          that.maxVal match {
            case Some(v2) => if (v2 > v1) that.maxVal else this.maxVal
            case _ => this.maxVal
          }
        }
        case _ => that.maxVal
      }
      new MinMaxCollector(newMin, newMax,
        if (this.lastChartTime < that.lastChartTime)
          this.lastChartTime
        else that.lastChartTime)
    }
  }
}
