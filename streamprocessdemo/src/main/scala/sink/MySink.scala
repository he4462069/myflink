package sink

import bean.SensorReading
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySink extends RichSinkFunction[SensorReading]{

}
