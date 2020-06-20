package function

import java.{lang, util}

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MyProcessWindowFunction extends ProcessWindowFunction[SensorReading,(String,Double,Long), String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[(String, Double, Long)]): Unit = {
    var sum:Double=0
    var cnt:Int = 0

    elements.foreach{data =>
      sum = sum +data.temperature
      cnt = cnt +1
    }


    val avg = sum/cnt

    val windowEnd = context.window.getEnd

    out.collect((key,avg,windowEnd))
  }
}
object MyProcessWindowFunction{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[SensorReading] = env.readTextFile("D:\\IdeaProjects\\myflink\\streamprocessdemo\\src\\main\\resources\\sqlDemoCsv.txt")
      .map { data =>
        val strings: Array[String] = data.split(",")
        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
      }

    val value: DataStream[(String, Double, Long)] = inputStream.keyBy(_.id).timeWindow(Time.milliseconds(10)).process(new MyProcessWindowFunction())
    value.print()

    env.execute("")
  }
}
