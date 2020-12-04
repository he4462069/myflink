package source

import bean.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable._
import scala.util.Random

class SensorSource extends SourceFunction[SensorReading]{

  private var flag = true
  val rand = new Random()

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    while (flag) {
      val curTemp: IndexedSeq[(String, Double)] = 1.to(10).map(i => (s"sensor_$i", 65 + rand.nextGaussian() * 20))
      val curTime: Long = System.currentTimeMillis()

      curTemp.foreach{temp=>
        ctx.collect(SensorReading(temp._1,curTime,temp._2))
      }
        Thread.sleep(100L)
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}
