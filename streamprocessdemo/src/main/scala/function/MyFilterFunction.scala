package function

import bean.SensorReading
import org.apache.flink.api.common.functions.FilterFunction

class MyFilterFunction extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.temperature>70
  }
}
