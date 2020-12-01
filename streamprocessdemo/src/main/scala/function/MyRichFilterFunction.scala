package function

import bean.SensorReading
import org.apache.flink.api.common.functions.{RichFilterFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.WindowFunction

/**
 * 所有的 Flink 函数都有相应的Rich版本，可以获取运行环境的上下文和一些生命周期方法，可以实现更复杂的功能
 */
class MyRichFilterFunction extends RichFilterFunction[SensorReading]{
  var subtaskIndex = 0

  override def open(parameters: Configuration): Unit = {
     subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
    val state: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("state", classOf[String]))
  }

  override def close(): Unit ={

  }

  override def filter(value: SensorReading): Boolean = {
    value.temperature>70
  }
}

