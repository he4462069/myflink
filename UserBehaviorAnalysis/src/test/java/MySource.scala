import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

class MySource extends SourceFunction[SensorReading]{

  //数据源是否在正常运行
  var isRunning:Boolean= true;

  val rand = new Random()


  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    var curTemp= 1 to 10 map (x => (s"sensor_$x", 65 + rand.nextGaussian() * 20))
    while (isRunning) {
      curTemp = curTemp.map(x =>(x._1,x._2+rand.nextGaussian()))

      val curTime = System.currentTimeMillis()

      curTemp.foreach{t =>
        ctx.collect(SensorReading(t._1,curTime,t._2))
      }

      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object Main{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input: DataStream[SensorReading] = env.addSource(new MySource)

    input.map(new MyMap).print("result")

    env.execute()
  }
}

class MyMap extends RichMapFunction[SensorReading,String] {

  override def open(parameters: Configuration): Unit = {
    println(System.currentTimeMillis())
  }

  override def map(value: SensorReading): String = {
    value.toString
  }
}