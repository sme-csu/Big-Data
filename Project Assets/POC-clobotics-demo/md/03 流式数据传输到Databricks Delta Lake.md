# 3 流式数据传输到 Databricks Delta Lake

在大数据背景下，物联网设备数据算是企业数据资产中占据大量比例。对这些大量数据进行管理可以说式构建成功IOT平台的第一公里。
物联网数据平台可以在内部部署或在云上部署，作为云计算从业者，我更加喜欢基于云的解决方案，特别是PaaS产品。在本文中，我将展示如何使用Azure构建简易的流式数据处理链。

![05](https://i.loli.net/2021/02/22/ru28IyPHvFx5iNW.png)

## Databricks：统一分析平台和Delta Lake

Databricks提供了一个统一的数据工程、数据科学和业务逻辑平台。从根本上将，这是Azure提供的云计算商用Spark PaaS产品，可以帮助我们加快数据探索和准备工作。

## 那什么是Delta Lake呢？

Delta Lake是Databricks开源的一种存储层，可以将ACID事务引入大数据场景作为工作负载。在现有大数据生态存储机制中（例如Parquet），我们存储在HDFS中的文件是不可变的，当我们需要去更新Parquet文件中的记录时，只能通过重写整个文件解决。但是如果我们使用Delta Lake进行存储，我们就可以轻松地编写更新语句。
在IOT和实时计算场景下，Delta Lake作为重点的原因是：我们能够在数据到达时，对流数据进行查询，不用再等待数据分区更新（HDFS重写场景）。
在这个解决方案中，我们将看到如何对Databricks进行设置，并如何使用Spark Streaming收集Azure IOTHub的记录，将它们写入Delta表中。

## 设置Databricks

我们通过Azure门户单击“创建资源”->”分析“->”Azure Databricks“

![06](https://i.loli.net/2021/02/22/rKpuYL6McvTOgih.png)

## 参考资料

https://docs.microsoft.com/zh-cn/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal#create-an-azure-databricks-workspace

## 收集IoT Hub的数据流

按照方案，我们需要在集群中安装“azure-eventhubs-spark_2.11:2.3.6”Maven库。我们只需要单击“Workspace”->“单击你的用户名”->“创建”->“Library”

![07](https://i.loli.net/2021/02/22/CpoWzbEJAFZisaQ.png)

选择Maven页签，在Coordinates框中输入包名，单击“Create”

提示：安装完成后，应重新启动，使其生效。

连接到IoT Hub，输出流：

```python
import org.apache.spark.eventhubs._
import  org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import  org.apache.spark.sql.functions.{ explode, split }

//我们需要指定IOTHub连接字符串与名称
val connectionString = ConnectionStringBuilder("--IOTHub连接字符串--")
  .setEventHubName("--IoTHub 名称--")
  .build
val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
```
上面的代码段可以构建一个指向我们之前创建的IoT Hub的连接字符串，只需要从Azure门户中获取到连接字符串就可以构建链接。

### 要查看传入数据如何运行

```
display(eventhubs)
```

### 提取设备数据并创建一个Spark SQL表

从上一步创建的DataFrame中提取body字段中的数据，并构建一个SQL内存表：

```python
val messages = eventhubs
  .withColumn("Time", $"enqueuedTime")
  .withColumn("Body", $"body".cast(StringType))
  .select("Time","Body")

val messagesDF = messages.select(get_json_object($"Body", "$.storeid").alias("storeid"),
                              get_json_object($"Body", "$.timestamp").alias("timestamp").cast(TimestampType),
                              get_json_object($"Body", "$.SKU").alias("SKU").cast(LongType))
```

将流数据写入至Delta表进行存储，或者可以在别的任务中使用处理好的内存表进行数据分析与调用：
```python
messagesDF.printSchema

messagesDF.createOrReplaceTempView("ods_iothub_data")

messagesDF.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
```