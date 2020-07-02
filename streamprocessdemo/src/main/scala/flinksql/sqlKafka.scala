package flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, Json, Kafka, Schema}

object sqlKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //从kafka读取数据
    streamTableEnv.connect(
      new Kafka().version("0.11")
        .topic("testTopic")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")

    //注册输出表，输出到ES
    streamTableEnv.connect(
      new Elasticsearch()
        .version("6")
        .host("localhost", 9200, "http")
        .index("sensor")
        .documentType("temp")
    )
    .inUpsertMode()   // 指定是 Upsert 模式
    .withFormat(new Json())
    .withSchema(
        new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
    ).createTemporaryTable("esOutputTable")

    // 输出到 Mysql
    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |  id varchar(20) not null,
        |  cnt bigint not null
        |) with (
        |  'connector.type' = 'jdbc',
        |  'connector.url' = 'jdbc:mysql://localhost:3306/test',
        |  'connector.table' = 'sensor_count',
        |  'connector.driver' = 'com.mysql.jdbc.Driver',
        |  'connector.username' = 'root',
        |  'connector.password' = '123456'
        |)
  """.stripMargin

    streamTableEnv.sqlUpdate(sinkDDL)

  //aggResultSqlTable.insertInto("jdbcOutputTable")



    val table: Table = streamTableEnv.sqlQuery("select * from inputTable")
    //val datastream: DataStream[(String, Long, Double)] = table.toAppendStream[(String, Long, Double)]
    //val datastream: DataStream[(String, Long, Double)] = streamTableEnv.toAppendStream[(String, Long, Double)](table)

    //table api 的调用
    val resultTable: Table = table.select("id,temperature").filter("id = 'sensor_1'")
    val datastream: DataStream[(String, Long, Double)] = resultTable.toAppendStream[(String, Long, Double)]

    //sql的方式
    val table1: Table = streamTableEnv.sqlQuery("select id,temperature from inputTable where id = 'sensor_1'")

    //创建临时视图，可以认为View和Table是等价的
    streamTableEnv.createTemporaryView("resultTable",table1)

    datastream.print()





    env.execute()
  }
}
