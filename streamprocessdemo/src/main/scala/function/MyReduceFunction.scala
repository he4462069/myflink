package function

import bean.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id,0,value1.temperature.min(value2.temperature))
  }
}

object MyReduceFunction{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }

    val value: DataStream[SensorReading] = inputStream.keyBy("id").reduce(new MyReduceFunction)

    value.print()

    env.execute("")
  }
}
