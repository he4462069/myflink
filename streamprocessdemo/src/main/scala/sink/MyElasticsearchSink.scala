package sink

import java.util

import bean.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import source.SensorSource

/**
 * elasticSearch的sink测试
 */
object MyElasticsearchSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[SensorReading] = env.addSource(new SensorSource)

    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("localhost",9200))

    val esSink: ElasticsearchSink[SensorReading] = new ElasticsearchSink.Builder[SensorReading](hosts, new EsSinkFunction).build()
    input.addSink(esSink)

    env.execute("")
  }
}

class EsSinkFunction extends ElasticsearchSinkFunction[SensorReading]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    println("saving data: " + t)
    val map = new util.HashMap[String, String]()
    map.put("data", t.toString)
    val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(map)
    requestIndexer.add(indexRequest)
    println("saved successfully")

  }
}
