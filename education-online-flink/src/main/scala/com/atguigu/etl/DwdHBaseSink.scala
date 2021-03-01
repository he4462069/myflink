package com.atguigu.etl



import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

class DwdHBaseSink extends RichSinkFunction[TopicAndValue]{
  var conn :Connection = _
  override def open(parameters: Configuration): Unit = {
    val conf  = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","")
    conf.set("hbase.zookeeper.property.clientPort","")
    conn = ConnectionFactory.createConnection(conf)
  }

  //写入hbase
  override def invoke(value: TopicAndValue, context: SinkFunction.Context[_]): Unit = {

  }

  override def close(): Unit = {
    conn.close()
  }
}
